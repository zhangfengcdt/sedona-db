use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::{
    distance::st_distance_impl, st_area::st_area_impl, st_buffer::st_buffer_impl,
    st_centroid::st_centroid_impl, st_dwithin::st_dwithin_impl, st_length::st_length_impl,
    st_perimeter::st_perimeter_impl,
};

use crate::binary_predicates::{
    st_contains_impl, st_covered_by_impl, st_covers_impl, st_disjoint_impl, st_equals_impl,
    st_intersects_impl, st_touches_impl, st_within_impl,
};

use crate::overlay::{
    st_difference_impl, st_intersection_impl, st_sym_difference_impl, st_union_impl,
};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_area", st_area_impl()),
        ("st_buffer", st_buffer_impl()),
        ("st_centroid", st_centroid_impl()),
        ("st_contains", st_contains_impl()),
        ("st_coveredby", st_covered_by_impl()),
        ("st_covers", st_covers_impl()),
        ("st_difference", st_difference_impl()),
        ("st_disjoint", st_disjoint_impl()),
        ("st_distance", st_distance_impl()),
        ("st_dwithin", st_dwithin_impl()),
        ("st_equals", st_equals_impl()),
        ("st_length", st_length_impl()),
        ("st_intersection", st_intersection_impl()),
        ("st_intersects", st_intersects_impl()),
        ("st_perimeter", st_perimeter_impl()),
        ("st_symdifference", st_sym_difference_impl()),
        ("st_touches", st_touches_impl()),
        ("st_union", st_union_impl()),
        ("st_within", st_within_impl()),
    ]
}
