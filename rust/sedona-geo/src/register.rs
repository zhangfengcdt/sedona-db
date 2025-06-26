use sedona_expr::aggregate_udf::SedonaAccumulatorRef;
use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::st_intersection_aggr::st_intersection_aggr_impl;
use crate::{st_area::st_area_impl, st_intersects::st_intersects_impl};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_intersects", st_intersects_impl()),
        ("st_area", st_area_impl()),
    ]
}

pub fn aggregate_kernels() -> Vec<(&'static str, SedonaAccumulatorRef)> {
    vec![("st_intersection_aggr", st_intersection_aggr_impl())]
}
