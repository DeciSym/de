//! The query page: YASGUI, embedded in the binary.

pub(super) const HTML_ROOT_PAGE: &str = include_str!("../../templates/query.html");

#[expect(clippy::large_include_file)]
pub(super) const YASGUI_JS: &str = include_str!("../../templates/yasgui/yasgui.min.js");

pub(super) const YASGUI_CSS: &str = include_str!("../../templates/yasgui/yasgui.min.css");

pub(super) const LOGO: &str = include_str!("../../templates/logo.svg");
