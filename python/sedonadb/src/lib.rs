use std::ffi::c_void;

use pyo3::{ffi::Py_uintptr_t, prelude::*};
use sedona_adbc::AdbcSedonadbDriverInit;

mod context;
mod dataframe;
mod error;
mod import_from;
mod reader;
mod runtime;
mod schema;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "mimalloc")]
fn configure_tg_allocator() {
    use libmimalloc_sys::{mi_free, mi_malloc, mi_realloc};
    use sedona_tg::tg::set_allocator;

    // Configure tg to use mimalloc
    unsafe { set_allocator(mi_malloc, mi_realloc, mi_free) }.expect("Failed to set tg allocator");
}

#[pyfunction]
fn sedona_python_version() -> PyResult<String> {
    Ok(VERSION.to_string())
}

#[pyfunction]
fn sedona_adbc_driver_init() -> PyResult<Py_uintptr_t> {
    let driver_init_void = AdbcSedonadbDriverInit as *const c_void;
    Ok(driver_init_void as Py_uintptr_t)
}

#[pymodule]
fn _lib(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[cfg(feature = "mimalloc")]
    configure_tg_allocator();

    m.add_function(wrap_pyfunction!(sedona_python_version, m)?)?;
    m.add_function(wrap_pyfunction!(sedona_adbc_driver_init, m)?)?;

    m.add_class::<context::InternalContext>()?;
    m.add_class::<dataframe::InternalDataFrame>()?;
    m.add("SedonaError", py.get_type::<error::SedonaError>())?;
    m.add_class::<schema::PySedonaSchema>()?;
    m.add_class::<schema::PySedonaField>()?;
    m.add_class::<schema::PySedonaType>()?;

    Ok(())
}
