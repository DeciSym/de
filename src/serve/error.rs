//! Error responses: an [`HttpError`] is a status and a message.

use super::body::ResponseBody;
use http::{HeaderValue, Response, StatusCode, header::CONTENT_TYPE};
use std::fmt;

/// A request the endpoint couldn't answer: the status, and a message for the
/// client, sent as plain text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    pub fn new(status: StatusCode, message: impl fmt::Display) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }

    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The response for this error, which is also logged to stderr.
    #[must_use]
    pub fn into_response(self) -> Response<ResponseBody> {
        eprintln!("ERROR {:?}: {}", self.status, self.message);
        let mut response = Response::new(ResponseBody::from(self.message));
        *response.status_mut() = self.status;
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
