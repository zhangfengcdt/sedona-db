use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_Intersects() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_intersects_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_intersects",
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_geometry_or_geography(),
            ],
            DataType::Boolean.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(st_intersects_doc()),
    )
}

fn st_intersects_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return true if geomA intersects geomB",
        "SELECT ST_Intersects(ST_Point(0.25 0.25), ST_GeomFromText('POLYGON ((0 0, 1 0, 0 1, 0 0))')) AS val",
    )
    .with_argument("geomA", "geometry: Input geometry or geography")
    .with_argument("geomB", "geometry: Input geometry or geography")
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_intersects_udf().into();
        assert_eq!(udf.name(), "st_intersects");
        assert!(udf.documentation().is_some())
    }
}
