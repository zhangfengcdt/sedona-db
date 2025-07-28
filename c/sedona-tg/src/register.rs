use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::binary_predicate;

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_equals", binary_predicate::st_equals_impl()),
        ("st_intersects", binary_predicate::st_intersects_impl()),
        ("st_disjoint", binary_predicate::st_disjoint_impl()),
        ("st_contains", binary_predicate::st_contains_impl()),
        ("st_within", binary_predicate::st_within_impl()),
        ("st_covers", binary_predicate::st_covers_impl()),
        ("st_coveredby", binary_predicate::st_covered_by_impl()),
        ("st_touches", binary_predicate::st_touches_impl()),
    ]
}
