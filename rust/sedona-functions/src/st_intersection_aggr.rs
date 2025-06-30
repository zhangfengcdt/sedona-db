use std::vec;

use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::{aggregate_udf::SedonaAggregateUDF, scalar_udf::ArgMatcher};
use sedona_schema::datatypes::{Edges, SedonaType};

/// ST_Intersection_Aggr() aggregate UDF implementation
///
/// An implementation of intersection calculation.
pub fn st_intersection_aggr_udf() -> SedonaAggregateUDF {
    SedonaAggregateUDF::new_stub(
        "st_intersection_aggr",
        ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            SedonaType::Wkb(Edges::Planar, None),
        ),
        Volatility::Immutable,
        Some(st_intersection_aggr_doc()),
    )
}

fn st_intersection_aggr_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the polygon intersection of all polygons in geom.",
        "ST_Intersection_Aggr (A: geometryColumn)",
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .with_sql_example(
        "SELECT ST_Intersection_Aggr(ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'))",
    )
    .build()
}

#[cfg(test)]
mod test {
    use datafusion_expr::AggregateUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: AggregateUDF = st_intersection_aggr_udf().into();
        assert_eq!(udf.name(), "st_intersection_aggr");
        assert!(udf.documentation().is_some());
    }
}
