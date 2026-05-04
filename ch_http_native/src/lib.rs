// Minimal HTTP client for clickhouse-arrow.
//
// Wraps `ureq::Agent` (blocking, no-tokio) behind a tiny PyO3 surface.
// Two response shapes:
//
//   * `Client.post(...)`            -- buffered: read the whole body into
//                                      bytes and return (status, body).
//   * `Client.post_streaming(...)`  -- streaming: return a `Response` whose
//                                      `read(n)` pulls bytes off the socket
//                                      on demand. Compatible with
//                                      `pyarrow.ipc.open_stream` so record
//                                      batches can be parsed as they arrive.
//
// Both paths release the GIL for the duration of network I/O.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB hard cap

enum HttpError {
    Transport(String),
    Io(std::io::Error),
}

fn map_http_err(e: HttpError) -> PyErr {
    match e {
        HttpError::Transport(m) => PyConnectionError::new_err(m),
        HttpError::Io(e) => PyIOError::new_err(e.to_string()),
    }
}

type BodyReader = Box<dyn Read + Send>;

#[pyclass(module = "ch_http_native._native", name = "Client", frozen)]
struct Client {
    agent: Arc<ureq::Agent>,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (timeout_seconds = None))]
    fn new(timeout_seconds: Option<f64>) -> Self {
        let mut builder = ureq::AgentBuilder::new();
        if let Some(secs) = timeout_seconds {
            builder = builder.timeout(Duration::from_secs_f64(secs));
        }
        Client {
            agent: Arc::new(builder.build()),
        }
    }

    /// POST `body` to `url` with `headers`, return `(status, response_body)`.
    ///
    /// Buffered: the full response body is read before returning.
    fn post<'py>(
        &self,
        py: Python<'py>,
        url: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> PyResult<(u16, Bound<'py, PyBytes>)> {
        let agent = Arc::clone(&self.agent);
        let url = url.to_owned();
        let body = body.to_vec();

        let (status, bytes) = py
            .allow_threads(move || -> Result<(u16, Vec<u8>), HttpError> {
                let (status, mut reader) = send(&agent, &url, &headers, &body)?;
                let mut buf = Vec::new();
                reader
                    .as_mut()
                    .take(MAX_RESPONSE_BYTES)
                    .read_to_end(&mut buf)
                    .map_err(HttpError::Io)?;
                Ok((status, buf))
            })
            .map_err(map_http_err)?;

        Ok((status, PyBytes::new_bound(py, &bytes)))
    }

    /// POST `body` to `url` with `headers`, return a streaming `Response`.
    ///
    /// The body is read from the socket lazily via `Response.read(size)`.
    fn post_streaming(
        &self,
        py: Python<'_>,
        url: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> PyResult<Response> {
        let agent = Arc::clone(&self.agent);
        let url = url.to_owned();
        let body = body.to_vec();

        let (status, reader) = py
            .allow_threads(move || send(&agent, &url, &headers, &body))
            .map_err(map_http_err)?;

        Ok(Response {
            status,
            reader: Mutex::new(Some(reader)),
        })
    }
}

fn send(
    agent: &ureq::Agent,
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<(u16, BodyReader), HttpError> {
    let mut req = agent.post(url);
    for (name, value) in headers {
        req = req.set(name, value);
    }
    match req.send_bytes(body) {
        Ok(r) => {
            let status = r.status();
            Ok((status, r.into_reader()))
        }
        Err(ureq::Error::Status(status, r)) => Ok((status, r.into_reader())),
        Err(e) => Err(HttpError::Transport(e.to_string())),
    }
}

/// A streaming HTTP response. File-like enough for `pyarrow.ipc.open_stream`.
#[pyclass(module = "ch_http_native._native", name = "Response")]
struct Response {
    #[pyo3(get)]
    status: u16,
    reader: Mutex<Option<BodyReader>>,
}

#[pymethods]
impl Response {
    /// HTTP status code (alias for `status`, mirrors urllib3/requests).
    #[getter]
    fn status_code(&self) -> u16 {
        self.status
    }

    /// Read up to `size` bytes from the response body.
    ///
    /// `size = None` or `-1` reads to EOF (capped at 10 GiB). A short read
    /// (fewer bytes than requested) signals EOF.
    #[pyo3(signature = (size = None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        size: Option<i64>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut guard = self
            .reader
            .lock()
            .map_err(|_| PyIOError::new_err("response reader poisoned"))?;
        let reader = guard
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("response is closed"))?;

        let buf = py
            .allow_threads(|| -> std::io::Result<Vec<u8>> {
                match size {
                    None | Some(-1) => {
                        let mut buf = Vec::new();
                        reader
                            .as_mut()
                            .take(MAX_RESPONSE_BYTES)
                            .read_to_end(&mut buf)?;
                        Ok(buf)
                    }
                    Some(n) if n >= 0 => {
                        let mut buf = vec![0u8; n as usize];
                        let mut total = 0;
                        while total < buf.len() {
                            match reader.read(&mut buf[total..]) {
                                Ok(0) => break,
                                Ok(k) => total += k,
                                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                                Err(e) => return Err(e),
                            }
                        }
                        buf.truncate(total);
                        Ok(buf)
                    }
                    Some(_) => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "size must be -1 or >= 0",
                    )),
                }
            })
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::InvalidInput {
                    PyValueError::new_err(e.to_string())
                } else {
                    PyIOError::new_err(e.to_string())
                }
            })?;

        Ok(PyBytes::new_bound(py, &buf))
    }

    fn close(&self) {
        if let Ok(mut guard) = self.reader.lock() {
            *guard = None;
        }
    }

    /// File-like surface expected by `pyarrow.ipc.open_stream`.
    #[getter]
    fn closed(&self) -> bool {
        match self.reader.lock() {
            Ok(g) => g.is_none(),
            Err(_) => true,
        }
    }

    fn readable(&self) -> bool {
        !self.closed()
    }

    fn writable(&self) -> bool {
        false
    }

    fn seekable(&self) -> bool {
        false
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Response>()?;
    Ok(())
}
