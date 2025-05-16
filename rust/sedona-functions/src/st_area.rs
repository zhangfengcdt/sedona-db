use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_Area() scalar UDF implementation
///
/// Stub function for area calculation.
pub fn st_area_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_area",
        ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Float64.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(st_area_doc()),
    )
}

fn st_area_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the area of a geometry",
        "SELECT ST_Area(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 0 1, 0 0))'))",
    )
    .with_argument("geom", "geometry: Input geometry")
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_area_udf().into();
        assert_eq!(udf.name(), "st_area");
        assert!(udf.documentation().is_some())
    }
}
