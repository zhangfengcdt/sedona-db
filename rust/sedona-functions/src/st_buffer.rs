use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};
use sedona_schema::datatypes::WKB_GEOMETRY;

/// ST_Buffer() scalar UDF stub
pub fn st_buffer_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_buffer",
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_numeric(),
            ],
            WKB_GEOMETRY,
        ),
        Volatility::Immutable,
        Some(st_buffer_doc()),
    )
}

fn st_buffer_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns a geometry that represents all points whose distance from this Geometry is less than or equal to distance",
        "st_buffer (A: Geometry, B: Double)"
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_argument("distance", "distance: Radius of the buffer")
    .with_sql_example("SELECT ST_Buffer(ST_GeomFromText('POLYGON ((10 10, 11 10, 10 11, 10 10))'), 1.0)".to_string())
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_buffer_udf().into();
        assert_eq!(udf.name(), "st_buffer");
        assert!(udf.documentation().is_some())
    }
}
