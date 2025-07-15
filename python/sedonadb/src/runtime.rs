use std::{future::Future, time::Duration};

use pyo3::Python;
use tokio::{runtime::Runtime, time::sleep};

use crate::error::PySedonaError;

// Adapted from datafusion-python:
// https://github.com/apache/datafusion-python/blob/cbe845b1e840c78f7a9fc4d83d184a1e6f35f47c/src/utils.rs#L64
pub fn wait_for_future<F>(py: Python, runtime: &Runtime, fut: F) -> Result<F::Output, PySedonaError>
where
    F: Future + Send,
    F::Output: Send,
{
    const INTERVAL_CHECK_SIGNALS: Duration = Duration::from_millis(1_000);

    py.allow_threads(|| {
        runtime.block_on(async {
            tokio::pin!(fut);
            loop {
                tokio::select! {
                    res = &mut fut => break Ok(res),
                    _ = sleep(INTERVAL_CHECK_SIGNALS) => {
                        Python::with_gil(|py| py.check_signals())?;
                    }
                }
            }
        })
    })
}

// A version of the above except for use from an arbitrary Rust function instead
// of from somewhere that had already acquired the GIL.
pub fn wait_for_future_from_rust<F>(runtime: &Runtime, fut: F) -> Result<F::Output, PySedonaError>
where
    F: Future + Send,
    F::Output: Send,
{
    const INTERVAL_CHECK_SIGNALS: Duration = Duration::from_millis(1_000);

    runtime.block_on(async {
        tokio::pin!(fut);
        loop {
            tokio::select! {
                res = &mut fut => break Ok(res),
                _ = sleep(INTERVAL_CHECK_SIGNALS) => {
                    Python::with_gil(|py| py.check_signals())?;
                }
            }
        }
    })
}
