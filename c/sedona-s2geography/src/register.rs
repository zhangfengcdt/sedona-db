use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::scalar_kernel;

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_area", scalar_kernel::st_area_impl()),
        ("st_centroid", scalar_kernel::st_centroid_impl()),
        ("st_closestpoint", scalar_kernel::st_closest_point_impl()),
        ("st_contains", scalar_kernel::st_contains_impl()),
        ("st_convex_hull", scalar_kernel::st_convex_hull_impl()),
        ("st_difference", scalar_kernel::st_difference_impl()),
        ("st_distance", scalar_kernel::st_distance_impl()),
        ("st_equals", scalar_kernel::st_equals_impl()),
        ("st_intersection", scalar_kernel::st_intersection_impl()),
        ("st_intersects", scalar_kernel::st_intersects_impl()),
        (
            "st_lineinterpolatepoint",
            scalar_kernel::st_line_interpolate_point_impl(),
        ),
        (
            "st_linelocatepoint",
            scalar_kernel::st_line_locate_point_impl(),
        ),
        ("st_length", scalar_kernel::st_length_impl()),
        ("st_sym_difference", scalar_kernel::st_sym_difference_impl()),
        ("st_maxdistance", scalar_kernel::st_max_distance_impl()),
        ("st_perimeter", scalar_kernel::st_perimeter_impl()),
        ("st_shortestline", scalar_kernel::st_shortest_line_impl()),
        ("st_union", scalar_kernel::st_union_impl()),
    ]
}
