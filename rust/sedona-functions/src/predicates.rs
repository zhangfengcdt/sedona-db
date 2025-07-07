use arrow_schema::DataType;
use datafusion_expr::{scalar_doc_sections::DOC_SECTION_OTHER, Documentation, Volatility};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarUDF};

/// ST_Equals() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_equals_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Equals", "equals")
}

/// ST_Intersects() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_intersects_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Intersects", "intersects")
}

/// ST_Disjoint() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_disjoint_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Disjoint", "is disjoint from")
}

/// ST_Contains() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_contains_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Contains", "contains")
}

/// ST_Within() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_within_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Within", "is fully contained by")
}

/// ST_Covers() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_covers_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Covers", "covers")
}

/// ST_CoveredBy() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_covered_by_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_CoveredBy", "is covered by")
}

/// ST_Touches() scalar UDF implementation
///
/// Stub function for an intersection check.
pub fn st_touches_udf() -> SedonaScalarUDF {
    predicate_stub_udf("ST_Touches", "touches")
}

pub fn predicate_stub_udf(name: &str, action: &str) -> SedonaScalarUDF {
    SedonaScalarUDF::new_stub(
        &name.to_lowercase(),
        ArgMatcher::new(
            vec![
                ArgMatcher::is_geometry_or_geography(),
                ArgMatcher::is_geometry_or_geography(),
            ],
            DataType::Boolean.try_into().unwrap(),
        ),
        Volatility::Immutable,
        Some(predicate_doc(name, action)),
    )
}

fn predicate_doc(name: &str, action: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("Return true if geomA {action} geomB"),
        format!("{name} (A: Geometry, B: Geometry)")
    )
    .with_argument("geomA", "geometry: Input geometry or geography")
    .with_argument("geomB", "geometry: Input geometry or geography")
    .with_sql_example(format!("SELECT {name}(ST_Point(0.25 0.25), ST_GeomFromText('POLYGON ((0 0, 1 0, 0 1, 0 0))')) AS val"))
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
