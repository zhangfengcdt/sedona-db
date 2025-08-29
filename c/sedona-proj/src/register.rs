use sedona_expr::aggregate_udf::SedonaAccumulatorRef;
use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::st_transform::st_transform_impl;

pub use crate::st_transform::configure_global_proj_engine;
pub use crate::transform::ProjCrsEngineBuilder;

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![("st_transform", st_transform_impl())]
}

pub fn aggregate_kernels() -> Vec<(&'static str, SedonaAccumulatorRef)> {
    vec![]
}
