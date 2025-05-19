use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_Length() scalar UDF implementation
///
/// Stub function for length calculation.
pub fn st_length_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_length",
        ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Float64.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(st_length_doc()),
    )
}

fn st_length_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the length of geom\
         This function only supports LineString, MultiLineString, and GeometryCollections \
         containing linear geometries. Use ST_Perimeter for polygons.\
        ",
        "SELECT ST_Length(ST_GeomFromWKT('LINESTRING(38 16,38 50,65 50,66 16,38 16)'))",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_related_udf("ST_Perimeter")
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_length_udf().into();
        assert_eq!(udf.name(), "st_length");
        assert!(udf.documentation().is_some())
    }
}
