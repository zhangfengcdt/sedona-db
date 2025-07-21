use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::scalar_kernel;

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_length", scalar_kernel::st_length_impl()),
        ("st_intersects", scalar_kernel::st_intersects_impl()),
    ]
}
