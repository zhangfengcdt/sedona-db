use std::ffi::c_void;

use pyo3::{ffi::Py_uintptr_t, prelude::*};
use sedona_adbc::AdbcSedonaRsDriverInit;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn sedona_python_version() -> PyResult<String> {
    Ok(VERSION.to_string())
}

#[pyfunction]
fn sedona_adbc_driver_init() -> PyResult<Py_uintptr_t> {
    let driver_init_void = AdbcSedonaRsDriverInit as *const c_void;
    Ok(driver_init_void as Py_uintptr_t)
}

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sedona_python_version, m)?)?;
    m.add_function(wrap_pyfunction!(sedona_adbc_driver_init, m)?)?;
    Ok(())
}
