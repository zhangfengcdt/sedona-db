use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};
use sedona_schema::datatypes::WKB_GEOMETRY;

/// ST_LineLocatePoint() scalar UDF implementation
pub fn st_line_locate_point_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_line_locate_point",
        ArgMatcher::new(
            vec![ArgMatcher::is_geometry(), ArgMatcher::is_geometry()],
            DataType::Float64.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(st_line_locate_point_doc()),
    )
}

fn st_line_locate_point_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the distance along a linear geometry required to reach the closest point to target",
        "ST_LineLocatePoint (geom: Geometry, target: Geometry)",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_argument("target", "geometry: Point to locate")
    .with_sql_example(
        "SELECT ST_LineLocatePoint(ST_GeomFromWKT('LINESTRING(38 16, 38 50, 65 50, 66 16, 38 16)'), ST_Point(38, 50))",
    )
    .build()
}

/// ST_LineInterpolatePoint() scalar UDF implementation
pub fn st_line_interpolate_point_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        "st_line_interpolate_point",
        ArgMatcher::new(
            vec![ArgMatcher::is_geometry(), ArgMatcher::is_numeric()],
            WKB_GEOMETRY,
        ),
        Volatility::Immutable,
        Some(st_line_interpolate_point_doc()),
    )
}

fn st_line_interpolate_point_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the point at a given relative distance (0 to 1) along a linear geometry",
        "ST_LineInterpolatePoint (geom: Geometry, distance: double)",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_argument("distance", "double: Relative distance along geom")
    .with_sql_example(
        "SELECT ST_LineInterpolatePoint(ST_GeomFromWKT('LINESTRING(38 16, 38 50, 65 50, 66 16, 38 16)'), 0.25)",
    )
    .build()
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_line_interpolate_point_udf().into();
        assert_eq!(udf.name(), "st_line_interpolate_point");
        assert!(udf.documentation().is_some());

        let udf: ScalarUDF = st_line_locate_point_udf().into();
        assert_eq!(udf.name(), "st_line_locate_point");
        assert!(udf.documentation().is_some());
    }
}
