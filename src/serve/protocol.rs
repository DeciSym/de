//! The SPARQL protocol's request side: the URL, the body, and the query parameters.

use super::{
    error::{HttpError, bad_request, internal_server_error},
    stream::evaluate_sparql_query,
};
use crate::sparql::AggregateHdt;
use http::{Request, Response, header::CONTENT_TYPE, uri::PathAndQuery};
use log::debug;
use oxhttp::model::Body;
use std::io::Read;
use url::form_urlencoded;

const MAX_SPARQL_BODY_SIZE: u64 = 1024 * 1024 * 128; // 128MB

pub(super) fn base_url(request: &Request<Body>) -> String {
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

pub(super) fn url_query(request: &Request<Body>) -> &[u8] {
    request.uri().query().unwrap_or_default().as_bytes()
}

pub(super) fn limited_string_body(request: &mut Request<Body>) -> Result<String, HttpError> {
    String::from_utf8(limited_body(request)?)
        .map_err(|e| bad_request(format!("Invalid UTF-8 body: {e}")))
}

pub(super) fn limited_body(request: &mut Request<Body>) -> Result<Vec<u8>, HttpError> {
    let body = request.body_mut();
    if let Some(body_len) = body.len() {
        if body_len > MAX_SPARQL_BODY_SIZE {
            // it's too big
            return Err(bad_request(format!(
                "SPARQL body payloads are limited to {MAX_SPARQL_BODY_SIZE} bytes, found {body_len} bytes"
            )));
        }
        let mut payload = Vec::with_capacity(
            body_len
                .try_into()
                .map_err(|_| bad_request("Huge body size"))?,
        );
        body.read_to_end(&mut payload)
            .map_err(internal_server_error)?;
        Ok(payload)
    } else {
        let mut payload = Vec::new();
        body.take(MAX_SPARQL_BODY_SIZE + 1)
            .read_to_end(&mut payload)
            .map_err(internal_server_error)?;
        let max_len =
            usize::try_from(MAX_SPARQL_BODY_SIZE).map_err(|_| bad_request("Huge body size"))?;
        if payload.len() > max_len {
            return Err(bad_request(format!(
                "SPARQL body payloads are limited to {MAX_SPARQL_BODY_SIZE} bytes"
            )));
        }
        Ok(payload)
    }
}

pub(super) fn configure_and_evaluate_sparql_query(
    store: &AggregateHdt,
    encoded: &[&[u8]],
    mut query: Option<String>,
    request: &Request<Body>,
    default_use_default_graph_as_union: bool,
    // timeout: Option<Duration>,
) -> Result<Response<Body>, HttpError> {
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
    if default_graph_uris.is_empty() && named_graph_uris.is_empty() {
        use_default_graph_as_union |= default_use_default_graph_as_union;
    }
    let query = query.ok_or_else(|| bad_request("You should set the 'query' parameter"))?;
    evaluate_sparql_query(
        store,
        &query,
        use_default_graph_as_union,
        &default_graph_uris,
        &named_graph_uris,
        request,
        // timeout,
    )
}

pub(super) fn content_type(request: &Request<Body>) -> Option<String> {
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
