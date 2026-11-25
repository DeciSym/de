//! Error responses: an [`HttpError`] is a status and a message.

use super::body::ResponseBody;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode,
    header::{ALLOW, CONTENT_TYPE},
};
use std::fmt;

/// A request the endpoint couldn't answer: the status, and a message for the
/// client, sent as plain text, with any headers the status calls for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    status: StatusCode,
    message: String,
    // Boxed so that a `Result` with this error stays small.
    headers: Box<HeaderMap>,
}

impl HttpError {
    pub fn new(status: StatusCode, message: impl fmt::Display) -> Self {
        Self {
            status,
            message: message.to_string(),
            headers: Box::default(),
        }
    }

    /// A 405 for a request to `path` with a method it doesn't accept, with
    /// the `Allow` header listing those it does.
    #[must_use]
    pub fn method_not_allowed(method: &Method, path: &str, allowed: &[Method]) -> Self {
        let allow = allowed
            .iter()
            .map(Method::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let error = Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            format!("{method} is not allowed on {path}, only {allow}"),
        );
        match HeaderValue::from_str(&allow) {
            Ok(value) => error.with_header(ALLOW, value),
            Err(_) => error,
        }
    }

    /// Adds a header to the error's response, such as `Allow` on a 405.
    #[must_use]
    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.append(name, value);
        self
    }

    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The response for this error, which is also logged to stderr.
    #[must_use]
    pub fn into_response(self) -> Response<ResponseBody> {
        eprintln!("ERROR {:?}: {}", self.status, self.message);
        let mut response = Response::new(ResponseBody::from(self.message));
        *response.status_mut() = self.status;
        *response.headers_mut() = *self.headers;
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for HttpError {}

pub(super) fn bad_request(message: impl fmt::Display) -> HttpError {
    eprintln!("BAD REQUEST: {message}");
    HttpError::new(StatusCode::BAD_REQUEST, message)
}

pub(super) fn not_implemented(message: impl fmt::Display) -> HttpError {
    eprintln!("NOT IMPLEMENTED: {message}");
    HttpError::new(StatusCode::NOT_IMPLEMENTED, message)
}

pub(super) fn unsupported_media_type(content_type: &str) -> HttpError {
    eprintln!("Unsupported Media Type: {content_type}");
    HttpError::new(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!("No supported content Content-Type given: {content_type}"),
    )
}

pub(super) fn internal_server_error(message: impl fmt::Display) -> HttpError {
    eprintln!("Internal server error: {message}");
    HttpError::new(StatusCode::INTERNAL_SERVER_ERROR, message)
}
