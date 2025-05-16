use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::{st_area::st_area_impl, st_intersects::st_intersects_impl};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_intersects", st_intersects_impl()),
        ("st_area", st_area_impl()),
    ]
}
