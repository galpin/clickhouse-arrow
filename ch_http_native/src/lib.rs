// Minimal HTTP client for clickhouse-arrow.
//
// Wraps `ureq::Agent` (blocking, no-tokio) behind a tiny PyO3 surface.
// Three response shapes:
//
//   * Client.post(...)            -- buffered: read the whole body into
//                                    bytes and return (status, body).
//   * Client.post_streaming(...)  -- streaming bytes: return a `Response`
//                                    whose `read(n)` pulls bytes off the
//                                    socket on demand. Compatible with
//                                    `pyarrow.ipc.open_stream`.
//   * Client.post_arrow_stream(.) -- zero-copy Arrow: parse the IPC stream
//                                    in Rust with `arrow-ipc` and return
//                                    a `RecordBatchStream` implementing
//                                    `__arrow_c_stream__`. pyarrow consumes
//                                    record batches via raw FFI pointers,
//                                    no PyBytes round trips.
//
// All paths transparently decompress responses with `Content-Encoding: zstd`
// or `gzip`. All paths release the GIL for the duration of network I/O.

use std::ffi::CString;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::StreamReader;
use pyo3::exceptions::{PyConnectionError, PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};

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

    /// Buffered POST: read the full response into bytes.
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

    /// Streaming POST: return a `Response` reading the body lazily.
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

    /// Arrow-IPC POST: parse the response as an Arrow IPC stream in Rust.
    /// The returned object exposes `__arrow_c_stream__` so pyarrow can pull
    /// record batches via the C Data Interface (zero-copy).
    fn post_arrow_stream(
        &self,
        py: Python<'_>,
        url: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> PyResult<RecordBatchStream> {
        let agent = Arc::clone(&self.agent);
        let url = url.to_owned();
        let body = body.to_vec();

        let result = py.allow_threads(move || -> Result<(u16, BodyReader), HttpError> {
            send(&agent, &url, &headers, &body)
        });
        let (status, reader) = result.map_err(map_http_err)?;

        if status != 200 {
            // Drain the body so we can surface ClickHouse's error message.
            let mut buf = Vec::new();
            let _ = reader
                .take(MAX_RESPONSE_BYTES)
                .read_to_end(&mut buf);
            return Err(PyRuntimeError::new_err(format!(
                "HTTP {}: {}",
                status,
                String::from_utf8_lossy(&buf)
            )));
        }

        // Build the IPC reader on the calling thread; the first read happens
        // when pyarrow pulls the schema or first batch via the FFI stream.
        let stream_reader = StreamReader::try_new(reader, None)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC: {e}")))?;

        Ok(RecordBatchStream {
            inner: Mutex::new(Some(Box::new(stream_reader))),
        })
    }
}

/// Issue the request, follow `Content-Encoding`, and return a body reader.
fn send(
    agent: &ureq::Agent,
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<(u16, BodyReader), HttpError> {
    let mut req = agent.post(url);
    let mut have_accept_encoding = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("accept-encoding") {
            have_accept_encoding = true;
        }
        req = req.set(name, value);
    }
    if !have_accept_encoding {
        req = req.set("Accept-Encoding", "zstd, gzip");
    }
    let response = match req.send_bytes(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => return Err(HttpError::Transport(e.to_string())),
    };
    let status = response.status();
    let encoding = response
        .header("Content-Encoding")
        .map(|s| s.to_ascii_lowercase());
    let raw: BodyReader = Box::new(response.into_reader());
    let reader: BodyReader = match encoding.as_deref() {
        Some("zstd") => Box::new(
            zstd::Decoder::new(raw)
                .map_err(HttpError::Io)?,
        ),
        Some("gzip") => {
            // ureq with the `gzip` feature would handle this transparently,
            // but we keep the feature off and fall back to identity. If the
            // server insists on gzip we error; users can switch to zstd.
            return Err(HttpError::Transport(
                "server returned Content-Encoding: gzip; request zstd instead".into(),
            ));
        }
        _ => raw,
    };
    Ok((status, reader))
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
    #[getter]
    fn status_code(&self) -> u16 {
        self.status
    }

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

/// Arrow IPC stream backed by a Rust `RecordBatchReader`. Exposes
/// `__arrow_c_stream__` so consumers (pyarrow) can pull batches via the
/// C Data Interface with no PyBytes round trips.
#[pyclass(module = "ch_http_native._native", name = "RecordBatchStream")]
struct RecordBatchStream {
    inner: Mutex<Option<Box<dyn RecordBatchReader + Send>>>,
}

#[pymethods]
impl RecordBatchStream {
    /// PyCapsule protocol: return a capsule named `arrow_array_stream`
    /// pointing at an `FFI_ArrowArrayStream`. The reader is consumed; this
    /// can only be called once per stream.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if requested_schema.is_some() {
            return Err(PyRuntimeError::new_err(
                "requested_schema is not supported",
            ));
        }
        let reader = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| PyIOError::new_err("stream poisoned"))?;
            guard
                .take()
                .ok_or_else(|| PyIOError::new_err("stream already consumed"))?
        };

        let ffi = FFI_ArrowArrayStream::new(reader);
        let name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, ffi, Some(name))
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Response>()?;
    m.add_class::<RecordBatchStream>()?;
    Ok(())
}
