// Minimal HTTP client for clickhouse-arrow.
//
// Wraps `ureq::Agent` (blocking, no-tokio) behind a tiny PyO3 surface.
// Optimized for one shape of request: POST a byte body, read the full
// response into bytes. Releases the GIL for the duration of the I/O so
// many Python threads can drive one client in parallel.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyIOError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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

    /// POST `body` to `url` with `headers`, return (status, response_body).
    ///
    /// `headers` is a list of (name, value) string tuples.
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
                let mut req = agent.post(&url);
                for (name, value) in &headers {
                    req = req.set(name, value);
                }
                let response = match req.send_bytes(&body) {
                    Ok(r) => r,
                    Err(ureq::Error::Status(status, r)) => {
                        let mut buf = Vec::new();
                        r.into_reader()
                            .take(MAX_RESPONSE_BYTES)
                            .read_to_end(&mut buf)
                            .map_err(HttpError::Io)?;
                        return Ok((status, buf));
                    }
                    Err(e) => return Err(HttpError::Transport(e.to_string())),
                };
                let status = response.status();
                let mut buf = Vec::new();
                response
                    .into_reader()
                    .take(MAX_RESPONSE_BYTES)
                    .read_to_end(&mut buf)
                    .map_err(HttpError::Io)?;
                Ok((status, buf))
            })
            .map_err(|e| match e {
                HttpError::Transport(m) => PyConnectionError::new_err(m),
                HttpError::Io(e) => PyIOError::new_err(e.to_string()),
            })?;

        Ok((status, PyBytes::new_bound(py, &bytes)))
    }
}

const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB hard cap

enum HttpError {
    Transport(String),
    Io(std::io::Error),
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
