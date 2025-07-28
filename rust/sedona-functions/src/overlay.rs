use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};
use sedona_schema::datatypes::WKB_GEOMETRY;

/// ST_Intersection() scalar UDF stub
pub fn st_intersection_udf() -> SedonaScalarUDF {
    overlay_stub_udf("ST_Intersection", "Intersection")
}

/// ST_Union() scalar UDF stub
pub fn st_union_udf() -> SedonaScalarUDF {
    overlay_stub_udf("ST_Union", "Union")
}

/// ST_Difference() scalar UDF stub
pub fn st_difference_udf() -> SedonaScalarUDF {
    overlay_stub_udf("ST_Difference", "Difference")
}

/// ST_SymDifference() scalar UDF stub
pub fn st_sym_difference_udf() -> SedonaScalarUDF {
    overlay_stub_udf("ST_SymDifference", "Symmetric difference")
}

pub fn overlay_stub_udf(name: &str, action: &str) -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        &name.to_lowercase(),
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_geometry_or_geography(),
            ],
            WKB_GEOMETRY,
        ),
        Volatility::Immutable,
        Some(overlay_doc(name, action)),
    )
}

fn overlay_doc(name: &str, action: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("{action} between geomA and geomB"),
        format!("{name} (A: Geometry, B: Geometry)")
    )
    .with_argument("geomA", "geometry: Input geometry or geography")
    .with_argument("geomB", "geometry: Input geometry or geography")
    .with_sql_example(format!("SELECT {name}(ST_GeomFromText('POLYGON ((1 1, 11 1, 1 11, 0 0))'), ST_GeomFromText('POLYGON ((0 0, 10 0, 0 10, 0 0))')) AS val"))
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_intersection_udf().into();
        assert_eq!(udf.name(), "st_intersection");
        assert!(udf.documentation().is_some())
    }
}
