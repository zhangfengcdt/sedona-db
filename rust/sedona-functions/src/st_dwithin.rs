use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_DWithin() scalar UDF stub
pub fn st_dwithin_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_dwithin",
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_numeric(),
            ],
            DataType::Boolean.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(dwithin_doc()),
    )
}

fn dwithin_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return true if geomA is within distance of geomB",
        "ST_DWithin (A: Geometry, B: Geometry, distance: Double)"
    )
    .with_argument("geomA", "geometry: Input geometry or geography")
    .with_argument("geomB", "geometry: Input geometry or geography")
    .with_argument("distance", "double: Distance in units of the geometry's coordinate system")
    .with_sql_example("SELECT ST_DWithin(ST_Point(0.25 0.25), ST_GeomFromText('POLYGON ((0 0, 1 0, 0 1, 0 0))'), 0.5)")
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_dwithin_udf().into();
        assert_eq!(udf.name(), "st_dwithin");
        assert!(udf.documentation().is_some())
    }
}
