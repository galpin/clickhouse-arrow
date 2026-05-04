// Minimal HTTP client for clickhouse-arrow.
//
// Wraps `ureq::Agent` (blocking, no-tokio) behind a tiny PyO3 surface:
//
//   * Client.post(url, headers, body) -> (status, bytes)
//       Buffered POST. Used for execute() and insert(): the response body
//       is read fully before returning.
//
//   * Client.post_arrow_stream(url, headers, body) -> RecordBatchStream
//       Streaming Arrow-IPC POST. Used for open_stream() / read_batches().
//       The IPC stream is parsed in Rust with arrow-ipc; the returned
//       object exposes __arrow_c_stream__, so pyarrow consumes record
//       batches via the C Data Interface (no PyBytes round trips).
//
// Both paths release the GIL during network I/O so a shared Client can
// be driven from many Python threads in parallel.

use std::ffi::CString;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::StreamReader;
use pyo3::exceptions::{PyConnectionError, PyIOError, PyRuntimeError};
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

#[pyclass(module = "clickhouse_arrow._native", name = "Client", frozen)]
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
            .detach(move || -> Result<(u16, Vec<u8>), HttpError> {
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

        Ok((status, PyBytes::new(py, &bytes)))
    }

    /// Arrow-IPC POST: parse the response as an Arrow IPC stream in Rust
    /// and return a `RecordBatchStream` exposing `__arrow_c_stream__`.
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

        let (status, reader) = py
            .detach(move || send(&agent, &url, &headers, &body))
            .map_err(map_http_err)?;

        if status != 200 {
            // Drain the body so we can surface ClickHouse's error message.
            let mut buf = Vec::new();
            let mut reader = reader;
            let _ = reader.as_mut().take(MAX_RESPONSE_BYTES).read_to_end(&mut buf);
            return Err(PyRuntimeError::new_err(format!(
                "HTTP {}: {}",
                status,
                String::from_utf8_lossy(&buf)
            )));
        }

        let stream_reader = StreamReader::try_new(reader, None)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC: {e}")))?;

        Ok(RecordBatchStream {
            inner: Mutex::new(Some(Box::new(stream_reader))),
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
    let response = match req.send_bytes(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => return Err(HttpError::Transport(e.to_string())),
    };
    let status = response.status();
    Ok((status, Box::new(response.into_reader())))
}

/// Arrow IPC stream backed by a Rust `RecordBatchReader`. Exposes
/// `__arrow_c_stream__` so pyarrow can pull batches via the C Data
/// Interface with no PyBytes round trips.
#[pyclass(module = "clickhouse_arrow._native", name = "RecordBatchStream")]
struct RecordBatchStream {
    inner: Mutex<Option<Box<dyn RecordBatchReader + Send>>>,
}

#[pymethods]
impl RecordBatchStream {
    /// PyCapsule protocol: return a capsule named `arrow_array_stream`
    /// pointing at an `FFI_ArrowArrayStream`. Consumes the reader; can
    /// only be called once per stream.
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
        PyCapsule::new(py, ffi, Some(name))
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<RecordBatchStream>()?;
    Ok(())
}
