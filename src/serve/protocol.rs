//! The SPARQL protocol's request side: the URL, the body, and the query parameters.

use super::{
    body::{RequestBody, ResponseBody},
    dataset::{DatasetSelection, DatasetSource},
    error::{HttpError, bad_request, internal_server_error},
    service::ServiceOptions,
    stream::evaluate_sparql_query,
};
use http::{
    Request, Response,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    uri::PathAndQuery,
};
use log::debug;
use std::{io::Read, sync::Arc};
use url::form_urlencoded;

pub(super) fn base_url(request: &Request<RequestBody>) -> String {
    let uri = request.uri();
    if uri.query().is_some() {
        // We remove the query
        let mut parts = uri.clone().into_parts();
        if let Some(path_and_query) = &mut parts.path_and_query
            && path_and_query.query().is_some()
        {
            if let Ok(path_only) = PathAndQuery::try_from(path_and_query.path()) {
                *path_and_query = path_only;
            } else {
                return uri.path().to_string();
            }
        }
        http::Uri::from_parts(parts)
            .map_or_else(|_| uri.path().to_string(), |built| built.to_string())
    } else {
        uri.to_string()
    }
}

pub(super) fn url_query(request: &Request<RequestBody>) -> &[u8] {
    request.uri().query().unwrap_or_default().as_bytes()
}

pub(super) fn limited_string_body(
    request: &mut Request<RequestBody>,
    max_bytes: u64,
) -> Result<String, HttpError> {
    String::from_utf8(limited_body(request, max_bytes)?)
        .map_err(|e| bad_request(format!("Invalid UTF-8 body: {e}")))
}

pub(super) fn limited_body(
    request: &mut Request<RequestBody>,
    max_bytes: u64,
) -> Result<Vec<u8>, HttpError> {
    let declared = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    if let Some(body_len) = declared
        && body_len > max_bytes
    {
        // it's too big
        return Err(bad_request(format!(
            "SPARQL body payloads are limited to {max_bytes} bytes, found {body_len} bytes"
        )));
    }
    let capacity = declared
        .map_or(Ok(0), usize::try_from)
        .map_err(|_| bad_request("Huge body size"))?;
    let mut payload = Vec::with_capacity(capacity);
    request
        .body_mut()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut payload)
        .map_err(internal_server_error)?;
    let max_len = usize::try_from(max_bytes).map_err(|_| bad_request("Huge body size"))?;
    if payload.len() > max_len {
        return Err(bad_request(format!(
            "SPARQL body payloads are limited to {max_bytes} bytes"
        )));
    }
    Ok(payload)
}

pub(super) fn configure_and_evaluate_sparql_query(
    source: &Arc<dyn DatasetSource>,
    encoded: &[&[u8]],
    mut query: Option<String>,
    request: &Request<RequestBody>,
    options: &ServiceOptions,
) -> Result<Response<ResponseBody>, HttpError> {
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    let mut use_default_graph_as_union = false;
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "query" => {
                    if query.is_some() {
                        return Err(bad_request("Multiple query parameters provided"));
                    }
                    query = Some(v.into_owned());
                }
                "default-graph-uri" => default_graph_uris.push(v.into_owned()),
                "union-default-graph" => use_default_graph_as_union = true,
                "named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    let names_graphs = !default_graph_uris.is_empty() || !named_graph_uris.is_empty();
    if use_default_graph_as_union && names_graphs {
        return Err(bad_request(
            "default-graph-uri or named-graph-uri and union-default-graph should not be set at the same time",
        ));
    }
    if !names_graphs {
        use_default_graph_as_union |= options.union_default_graph;
    }
    let query = query.ok_or_else(|| bad_request("You should set the 'query' parameter"))?;
    let selection = DatasetSelection {
        default_graph_uris,
        named_graph_uris,
        union_default_graph: use_default_graph_as_union,
    };
    evaluate_sparql_query(source, &query, selection, request, options.query_timeout)
}

pub(super) fn content_type(request: &Request<RequestBody>) -> Option<String> {
    let value = request.headers().get(CONTENT_TYPE)?.to_str().ok()?;
    debug!("request content_type: {value}");
    Some(
        value
            .split_once(';')
            .map_or(value, |(b, _)| b)
            .trim()
            .to_ascii_lowercase(),
    )
}
