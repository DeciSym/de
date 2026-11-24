//! Error responses: an `HttpError` is a status and a message.

use http::{HeaderValue, Response, StatusCode, header::CONTENT_TYPE};
use oxhttp::model::Body;
use std::fmt;

pub(super) type HttpError = (StatusCode, String);

pub(super) fn error(status: StatusCode, message: impl fmt::Display) -> Response<Body> {
    eprintln!("ERROR {status:?}: {message}");
    let mut response = Response::new(message.to_string().into());
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

pub(super) fn bad_request(message: impl fmt::Display) -> HttpError {
    eprintln!("BAD REQUEST: {message}");
    (StatusCode::BAD_REQUEST, message.to_string())
}

pub(super) fn not_implemented(message: impl fmt::Display) -> HttpError {
    eprintln!("NOT IMPLEMENTED: {message}");
    (StatusCode::NOT_IMPLEMENTED, message.to_string())
}

pub(super) fn unsupported_media_type(content_type: &str) -> HttpError {
    eprintln!("Unsupported Media Type: {content_type}");
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!("No supported content Content-Type given: {content_type}"),
    )
}

pub(super) fn internal_server_error(message: impl fmt::Display) -> HttpError {
    eprintln!("Internal server error: {message}");
    (StatusCode::INTERNAL_SERVER_ERROR, message.to_string())
}
