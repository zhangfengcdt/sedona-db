use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};
use sedona_schema::datatypes::WKB_GEOMETRY;

/// ST_Centroid() scalar UDF stub
///
/// Stub function for centroid calculation.
pub fn st_centroid_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_centroid",
        ArgMatcher::new(vec![ArgMatcher::is_geometry()], WKB_GEOMETRY),
        Volatility::Immutable,
        Some(st_centroid_doc()),
    )
}

fn st_centroid_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the centroid of geom",
        "ST_Centroid (A: Geometry)",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_sql_example("SELECT ST_Centroid(ST_GeomFromText('POLYGON ((1 1, 11 1, 1 11, 0 0))'))")
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_centroid_udf().into();
        assert_eq!(udf.name(), "st_centroid");
        assert!(udf.documentation().is_some())
    }
}
