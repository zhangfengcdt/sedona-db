use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::{st_area::st_area_impl, st_length::st_length_impl};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![("st_area", st_area_impl()), ("st_length", st_length_impl())]
}
