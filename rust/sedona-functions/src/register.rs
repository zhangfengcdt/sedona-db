use crate::{
    predicates::{
        st_contains_udf, st_covered_by_udf, st_covers_udf, st_disjoint_udf, st_equals_udf,
        st_intersects_udf, st_touches_udf, st_within_udf,
    },
    sd_format::sd_format_udf,
    st_analyze_aggr::st_analyze_aggr_udf,
    st_area::st_area_udf,
    st_asbinary::st_asbinary_udf,
    st_astext::st_astext_udf,
    st_envelope_aggr::st_envelope_aggr_udf,
    st_geomfromwkb::{st_geogfromwkb_udf, st_geomfromwkb_udf},
    st_geomfromwkt::{st_geogfromwkt_udf, st_geomfromwkt_udf},
    st_intersection_aggr::st_intersection_aggr_udf,
    st_length::st_length_udf,
    st_perimeter::st_perimeter_udf,
    st_point::{st_geogpoint_udf, st_point_udf},
    st_union_aggr::st_union_aggr_udf,
    st_xy::{st_x_udf, st_y_udf},
};
use sedona_expr::function_set::FunctionSet;

/// Export the set of functions defined in this crate
pub fn default_function_set() -> FunctionSet {
    let mut function_set = FunctionSet::new();

    function_set.insert_scalar_udf(sd_format_udf());
    function_set.insert_scalar_udf(st_area_udf());
    function_set.insert_scalar_udf(st_asbinary_udf());
    function_set.insert_scalar_udf(st_astext_udf());
    function_set.insert_scalar_udf(st_contains_udf());
    function_set.insert_scalar_udf(st_covered_by_udf());
    function_set.insert_scalar_udf(st_covers_udf());
    function_set.insert_scalar_udf(st_disjoint_udf());
    function_set.insert_scalar_udf(st_equals_udf());
    function_set.insert_scalar_udf(st_geogfromwkb_udf());
    function_set.insert_scalar_udf(st_geogfromwkt_udf());
    function_set.insert_scalar_udf(st_geogpoint_udf());
    function_set.insert_scalar_udf(st_geomfromwkb_udf());
    function_set.insert_scalar_udf(st_geomfromwkt_udf());
    function_set.insert_scalar_udf(st_intersects_udf());
    function_set.insert_scalar_udf(st_length_udf());
    function_set.insert_scalar_udf(st_perimeter_udf());
    function_set.insert_scalar_udf(st_point_udf());
    function_set.insert_scalar_udf(st_touches_udf());
    function_set.insert_scalar_udf(st_within_udf());
    function_set.insert_scalar_udf(st_x_udf());
    function_set.insert_scalar_udf(st_y_udf());

    function_set.insert_aggregate_udf(st_envelope_aggr_udf());
    function_set.insert_aggregate_udf(st_intersection_aggr_udf());
    function_set.insert_aggregate_udf(st_union_aggr_udf());
    function_set.insert_aggregate_udf(st_analyze_aggr_udf());

    function_set
}

/// Functions whose implementations are registered independently
///
/// These functions are included in the default function set; however,
/// it is useful to expose them individually for testing in crates that
/// implement them.
pub mod stubs {
    pub use crate::predicates::{
        st_contains_udf, st_covered_by_udf, st_covers_udf, st_disjoint_udf, st_equals_udf,
        st_intersects_udf, st_touches_udf, st_within_udf,
    };
    pub use crate::st_area::st_area_udf;
}
