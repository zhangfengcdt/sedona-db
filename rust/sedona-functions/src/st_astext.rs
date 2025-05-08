use std::{sync::Arc, vec};

use crate::executor::GenericExecutor;
use arrow_array::builder::StringBuilder;
use arrow_schema::DataType;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF};
use sedona_schema::datatypes::SedonaType;

/// ST_AsText() scalar UDF implementation
///
/// An implementation of WKT writing using GeoRust's wkt crate.
pub fn st_astext_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_astext",
        vec![Arc::new(STAsText {})],
        Volatility::Immutable,
        Some(st_astext_doc()),
    )
}

fn st_astext_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the Well-Known Text string representation of a geometry or geography",
        "SELECT ST_AsText(ST_Point(1.0, 2.0))",
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .with_related_udf("ST_GeomFromWKT")
    .build()
}

#[derive(Debug)]
struct STAsText {}

impl SedonaScalarKernel for STAsText {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Utf8.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        _: &SedonaType,
        args: &[ColumnarValue],
        _num_rows: usize,
    ) -> Result<ColumnarValue> {
        let executor = GenericExecutor::new(arg_types, args);

        // Estimate the minimum probable memory requirement of the output.
        // Here, the shortest full precision non-empty/non-null value would be
        // POINT (<16 digits> <16 digits>), or ~25 bytes.
        let min_probable_wkt_size = executor.num_iterations() * 25;

        // Initialize an output builder of the appropriate type
        let mut builder =
            StringBuilder::with_capacity(executor.num_iterations(), min_probable_wkt_size);

        executor.execute_wkb_void(|_i, maybe_item| {
            match maybe_item {
                Some(item) => {
                    wkt::to_wkt::write_geometry(&mut builder, &item)
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    builder.append_value("");
                }
                None => builder.append_null(),
            };

            Ok(())
        })?;

        // Create the output array
        executor.finish(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
    use sedona_schema::datatypes::{
        WKB_GEOGRAPHY, WKB_GEOMETRY, WKB_VIEW_GEOGRAPHY, WKB_VIEW_GEOMETRY,
    };
    use sedona_testing::{
        compare::assert_value_equal, create::create_array_value, create::create_scalar_value,
    };

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_astext_udf().into();
        assert_eq!(udf.name(), "st_astext");
        assert!(udf.documentation().is_some())
    }

    #[rstest]
    fn udf(
        #[values(WKB_GEOMETRY, WKB_GEOGRAPHY, WKB_VIEW_GEOMETRY, WKB_VIEW_GEOGRAPHY)]
        sedona_type: SedonaType,
    ) {
        let udf = st_astext_udf();

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(Some("POINT (1 2)"), &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Utf8(Some("POINT(1 2)".to_string()))),
        );

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(None, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        );

        let wkt_values = [Some("POINT(1 2)"), None, Some("POINT(3 5)")];
        let expected_array: StringArray = wkt_values.iter().collect();
        assert_value_equal(
            &udf.invoke_batch(&[create_array_value(&wkt_values, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Array(Arc::new(expected_array)),
        );
    }
}
