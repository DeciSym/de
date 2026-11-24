//! Choosing the result format from the Accept header.

use super::error::{HttpError, bad_request};
use http::{Request, StatusCode, header::ACCEPT};
use log::debug;
use oxhttp::model::Body;
use oxrdfio::RdfFormat;
use sparesults::QueryResultsFormat;
use std::str::FromStr;

pub(super) fn rdf_content_negotiation(request: &Request<Body>) -> Result<RdfFormat, HttpError> {
    content_negotiation(
        request,
        RdfFormat::from_media_type,
        RdfFormat::NQuads,
        &[
            ("application", RdfFormat::NQuads),
            ("text", RdfFormat::NQuads),
        ],
        "application/n-quads or text/turtle",
    )
}

pub(super) fn query_results_content_negotiation(
    request: &Request<Body>,
) -> Result<QueryResultsFormat, HttpError> {
    content_negotiation(
        request,
        QueryResultsFormat::from_media_type,
        QueryResultsFormat::Json,
        &[
            ("application", QueryResultsFormat::Json),
            ("text", QueryResultsFormat::Json),
        ],
        "application/sparql-results+json or text/tsv",
    )
}

fn content_negotiation<F: Copy>(
    request: &Request<Body>,
    parse: impl Fn(&str) -> Option<F>,
    default: F,
    default_by_base: &[(&str, F)],
    example: &str,
) -> Result<F, HttpError> {
    let header = request
        .headers()
        .get(ACCEPT)
        .map(|h| h.to_str())
        .transpose()
        .map_err(|_| bad_request("The Accept header should be a valid ASCII string"))?
        .unwrap_or_default();

    if header.is_empty() {
        debug!("accept header missing, using default content type");
        return Ok(default);
    }
    debug!("{ACCEPT}: {header}");
    let mut result = None;
    let mut result_score = 0_f32;
    for mut possible in header.split(',') {
        let mut score = 1.;
        if let Some((possible_type, last_parameter)) = possible.rsplit_once(';')
            && let Some((name, value)) = last_parameter.split_once('=')
            && name.trim().eq_ignore_ascii_case("q")
        {
            score = f32::from_str(value.trim())
                .map_err(|_| bad_request(format!("Invalid Accept media type score: {value}")))?;
            possible = possible_type;
        }
        if score <= result_score {
            continue;
        }
        let (possible_base, possible_sub) = possible
            .split_once(';')
            .unwrap_or((possible, ""))
            .0
            .split_once('/')
            .ok_or_else(|| bad_request(format!("Invalid media type: '{possible}'")))?;
        let possible_base = possible_base.trim();
        let possible_sub = possible_sub.trim();

        let mut format = None;
        if possible_base == "*" && possible_sub == "*" {
            format = Some(default);
        } else if possible_sub == "*" {
            for (base, sub_format) in default_by_base {
                if *base == possible_base {
                    format = Some(*sub_format);
                }
            }
        } else {
            format = parse(possible);
        }
        if let Some(format) = format {
            result = Some(format);
            result_score = score;
        }
    }
    result.ok_or_else(|| {
        eprintln!(
            "Not Acceptable: the accept header does not provide any accepted format like {example}"
        );
        (
            StatusCode::NOT_ACCEPTABLE,
            format!("The accept header does not provide any accepted format like {example}"),
        )
    })
}
