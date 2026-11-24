// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! A small HTTP/1.1 client that sends a request exactly as given.
//!
//! Clients that follow the Fetch standard, as oxhttp's does, drop "forbidden"
//! headers such as `Origin`, which the CORS cases must send. This one writes
//! every header it is given, one request per connection, and reads bodies
//! sent with `Content-Length`, with chunked encoding, or until the server
//! closes. A chunked body that ends early is a read error, which is how the
//! kit sees a response cut off.

use crate::target::KitResponse;
use anyhow::Context as _;
use http::header::{CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Sends `request` (whose URI is a path and query) to `authority` (`host:port`).
pub(crate) fn send(
    authority: &str,
    request: Request<Vec<u8>>,
    timeout: Duration,
) -> anyhow::Result<KitResponse> {
    let (parts, body) = request.into_parts();
    let stream =
        TcpStream::connect(authority).with_context(|| format!("connecting to {authority}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let mut head = format!("{} {} HTTP/1.1\r\n", parts.method, parts.uri);
    if !parts.headers.contains_key(HOST) {
        let _ = write!(head, "host: {authority}\r\n");
    }
    for (name, value) in &parts.headers {
        head.push_str(name.as_str());
        head.push_str(": ");
        head.push_str(value.to_str().context("a header value isn't ASCII")?);
        head.push_str("\r\n");
    }
    if !body.is_empty() || parts.method == Method::POST || parts.method == Method::PUT {
        let _ = write!(head, "content-length: {}\r\n", body.len());
    }
    head.push_str("connection: close\r\n\r\n");
    let mut writer = &stream;
    writer.write_all(head.as_bytes())?;
    writer.write_all(&body)?;
    writer.flush()?;

    let mut reader = BufReader::new(stream);
    let status = read_status(&mut reader)?;
    let headers = read_headers(&mut reader)?;
    let no_body = parts.method == Method::HEAD
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    let chunked = headers
        .get(TRANSFER_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let length = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    let body: Box<dyn Read> = if no_body {
        Box::new(io::empty())
    } else if chunked {
        Box::new(Chunked {
            reader,
            remaining: 0,
            done: false,
        })
    } else if let Some(length) = length {
        Box::new(Exact {
            reader: reader.take(length),
            missing: length,
        })
    } else {
        Box::new(reader)
    };
    Ok(KitResponse {
        status,
        headers,
        body,
    })
}

fn read_line(reader: &mut impl BufRead) -> io::Result<String> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the connection closed in the middle of the response",
        ));
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

fn read_status(reader: &mut impl BufRead) -> anyhow::Result<StatusCode> {
    let line = read_line(reader)?;
    let code = line
        .split_whitespace()
        .nth(1)
        .with_context(|| format!("not an HTTP status line: {line:?}"))?;
    Ok(StatusCode::from_bytes(code.as_bytes())?)
}

fn read_headers(reader: &mut impl BufRead) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    loop {
        let line = read_line(reader)?;
        if line.is_empty() {
            return Ok(headers);
        }
        let (name, value) = line
            .split_once(':')
            .with_context(|| format!("not a header: {line:?}"))?;
        headers.append(
            HeaderName::from_bytes(name.trim().as_bytes())?,
            HeaderValue::from_str(value.trim())?,
        );
    }
}

/// A body with a `Content-Length`: fewer bytes than that is a read error.
struct Exact<R> {
    reader: io::Take<R>,
    missing: u64,
}

impl<R: Read> Read for Exact<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(buf)?;
        if n == 0 && self.missing > 0 && !buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("the body ended {} bytes short", self.missing),
            ));
        }
        self.missing -= n as u64;
        Ok(n)
    }
}

/// A chunked body. A stream that ends before its last, empty chunk is a read
/// error.
struct Chunked<R> {
    reader: R,
    remaining: u64,
    done: bool,
}

impl<R: BufRead> Read for Chunked<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let line = read_line(&mut self.reader)?;
            let size = line.split(';').next().unwrap_or_default().trim();
            self.remaining = u64::from_str_radix(size, 16).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("not a chunk size: {line:?}"),
                )
            })?;
            if self.remaining == 0 {
                // Trailers, up to the empty line that ends the body.
                while !read_line(&mut self.reader)?.is_empty() {}
                self.done = true;
                return Ok(0);
            }
        }
        let wanted = usize::try_from(self.remaining).map_or(buf.len(), |r| r.min(buf.len()));
        let n = self.reader.read(&mut buf[..wanted])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the connection closed in the middle of a chunk",
            ));
        }
        self.remaining -= n as u64;
        if self.remaining == 0 {
            // Each chunk's data ends with a line break.
            read_line(&mut self.reader)?;
        }
        Ok(n)
    }
}
