use sedona_expr::{function_set::FunctionSet, scalar_udf::ScalarKernelRef};

use crate::{st_area::st_area_udf, st_intersects::st_intersects_impl};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![("st_intersects", st_intersects_impl())]
}

pub fn geo_function_set() -> FunctionSet {
    let mut function_set = FunctionSet::new();
    function_set.insert_scalar_udf(st_area_udf());
    function_set
}
