//! The query page: YASGUI, embedded in the binary.

use super::{
    body::{RequestBody, ResponseBody},
    error::{HttpError, internal_server_error},
    service::Routes,
};
use http::{Request, Response, header::CONTENT_TYPE};

const HTML_ROOT_PAGE: &str = include_str!("../../templates/query.html");

#[expect(clippy::large_include_file)]
const YASGUI_JS: &str = include_str!("../../templates/yasgui/yasgui.min.js");

const YASGUI_CSS: &str = include_str!("../../templates/yasgui/yasgui.min.css");

const LOGO: &str = include_str!("../../templates/logo.svg");

/// `de serve`'s query page: YASGUI at `/`, with its script, style sheet and
/// logo, for GET and HEAD.
pub struct Yasgui;

impl Routes for Yasgui {
    fn route(
        &self,
        request: &mut Request<RequestBody>,
    ) -> Option<Result<Response<ResponseBody>, HttpError>> {
        let (content_type, content) = match request.uri().path() {
            "/" => ("text/html", HTML_ROOT_PAGE),
            "/yasgui.min.css" => ("text/css", YASGUI_CSS),
            "/yasgui.min.js" => ("application/javascript", YASGUI_JS),
            "/logo.svg" => ("image/svg+xml", LOGO),
            _ => return None,
        };
        let body = match request.method().as_str() {
            "GET" => ResponseBody::from(content),
            "HEAD" => ResponseBody::Empty,
            _ => return None,
        };
        Some(
            Response::builder()
                .header(CONTENT_TYPE, content_type)
                .body(body)
                .map_err(internal_server_error),
        )
    }
}
