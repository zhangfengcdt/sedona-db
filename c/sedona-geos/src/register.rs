use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::{
    st_area::st_area_impl, st_centroid::st_centroid_impl, st_intersection::st_intersection_impl,
    st_length::st_length_impl,
};

use crate::binary_predicates::{
    st_contains_impl, st_covered_by_impl, st_covers_impl, st_disjoint_impl, st_equals_impl,
    st_intersects_impl, st_touches_impl, st_within_impl,
};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_area", st_area_impl()),
        ("st_centroid", st_centroid_impl()),
        ("st_contains", st_contains_impl()),
        ("st_coveredby", st_covered_by_impl()),
        ("st_covers", st_covers_impl()),
        ("st_disjoint", st_disjoint_impl()),
        ("st_equals", st_equals_impl()),
        ("st_length", st_length_impl()),
        ("st_intersection", st_intersection_impl()),
        ("st_intersects", st_intersects_impl()),
        ("st_touches", st_touches_impl()),
        ("st_within", st_within_impl()),
    ]
}
