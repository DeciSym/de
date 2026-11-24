//! Request and response bodies, independent of the HTTP server that carries
//! them: a server adapter wraps its own body types in these.

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Cursor, Read};

/// A request's body, read as the endpoint needs it.
pub struct RequestBody(Box<dyn Read>);

impl RequestBody {
    #[must_use]
    pub fn empty() -> Self {
        Self(Box::new(io::empty()))
    }

    /// A body read from `reader`, such as a server's own request body.
    pub fn from_reader(reader: impl Read + 'static) -> Self {
        Self(Box::new(reader))
    }
}

impl Default for RequestBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl Read for RequestBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl fmt::Debug for RequestBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestBody")
    }
}

impl From<Vec<u8>> for RequestBody {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_reader(Cursor::new(bytes))
    }
}

impl From<String> for RequestBody {
    fn from(text: String) -> Self {
        Self::from(text.into_bytes())
    }
}

impl From<&'static str> for RequestBody {
    fn from(text: &'static str) -> Self {
        Self::from_reader(text.as_bytes())
    }
}

/// A response's body.
pub enum ResponseBody {
    Empty,
    Bytes(Cow<'static, [u8]>),
    /// Read as it is produced, such as query results from the worker thread.
    /// A read error means the response must be cut off: the status has
    /// already been sent by then.
    Stream(Box<dyn Read + Send>),
}

impl ResponseBody {
    /// The body as a reader, for a server or test that consumes it as one.
    #[must_use]
    pub fn into_reader(self) -> Box<dyn Read + Send> {
        match self {
            Self::Empty => Box::new(io::empty()),
            Self::Bytes(bytes) => Box::new(Cursor::new(bytes)),
            Self::Stream(reader) => reader,
        }
    }
}

impl fmt::Debug for ResponseBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::Bytes(bytes) => write!(f, "Bytes({} bytes)", bytes.len()),
            Self::Stream(_) => f.write_str("Stream"),
        }
    }
}

impl From<Vec<u8>> for ResponseBody {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(Cow::Owned(bytes))
    }
}

impl From<String> for ResponseBody {
    fn from(text: String) -> Self {
        Self::from(text.into_bytes())
    }
}

impl From<&'static str> for ResponseBody {
    fn from(text: &'static str) -> Self {
        Self::Bytes(Cow::Borrowed(text.as_bytes()))
    }
}
