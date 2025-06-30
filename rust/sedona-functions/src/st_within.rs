use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_Within() scalar UDF implementation
///
/// Stub function for a within check.
pub fn st_within_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_within",
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_geometry_or_geography(),
            ],
            DataType::Boolean.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(st_within_doc()),
    )
}

fn st_within_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return true if geomA is fully contained by geomB",
        "ST_Within (A: Geometry, B: Geometry)",
    )
        .with_argument("geomA", "geometry: Input geometry or geography")
        .with_argument("geomB", "geometry: Input geometry or geography")
        .with_sql_example("SELECT ST_Within(ST_GeomFromWKT('POLYGON((1 1,2 1,2 2,1 2,1 1))'), ST_GeomFromWKT('POLYGON((0 0,3 0,3 3,0 3,0 0))')) as val")
        .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_within_udf().into();
        assert_eq!(udf.name(), "st_within");
        assert!(udf.documentation().is_some())
    }
}
