use std::{sync::Arc, vec};

use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_expr::udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF};
use sedona_schema::datatypes::SedonaType;

/// ST_AsBinary() scalar UDF implementation
///
/// An implementation of WKB writing using GeoRust's wkt crate.
pub fn st_asbinary_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_asbinary",
        vec![Arc::new(STAsBinary {})],
        Volatility::Immutable,
        Some(st_asbinary_doc()),
    )
}

fn st_asbinary_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the Well-Known Binary representation of a geometry or geography",
        "SELECT ST_AsBinary(ST_Point(1.0, 2.0))",
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .build()
}

#[derive(Debug)]
struct STAsBinary {}

impl SedonaScalarKernel for STAsBinary {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        // If we have WkbView input, return BinaryView to avoid a cast
        if args.len() == 1 {
            if let SedonaType::WkbView(_, _) = args[0] {
                return Ok(Some(SedonaType::Arrow(DataType::BinaryView)));
            }
        }

        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Binary.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        _: &[SedonaType],
        _: &SedonaType,
        args: &[ColumnarValue],
        _: usize,
    ) -> Result<ColumnarValue> {
        // This currently works because our return_type() ensure we didn't need a cast
        Ok(args[0].clone())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, BinaryViewArray};
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
    use sedona_schema::datatypes::{
        WKB_GEOGRAPHY, WKB_GEOMETRY, WKB_VIEW_GEOGRAPHY, WKB_VIEW_GEOMETRY,
    };
    use sedona_testing::{
        compare::assert_value_equal,
        create::{create_array_value, create_scalar_value},
    };

    use super::*;

    const POINT12: [u8; 21] = [
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    ];

    #[test]
    fn udf_metadata() {
        let udf: ScalarUDF = st_asbinary_udf().into();
        assert_eq!(udf.name(), "st_asbinary");
        assert!(udf.documentation().is_some())
    }

    #[rstest]
    fn udf_geometry_input(#[values(WKB_GEOMETRY, WKB_GEOGRAPHY)] sedona_type: SedonaType) {
        let udf: ScalarUDF = st_asbinary_udf().into();

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(Some("POINT (1 2)"), &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Binary(Some(POINT12.to_vec()))),
        );

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(None, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Binary(None)),
        );

        let expected_array: BinaryArray = [Some(POINT12), None, Some(POINT12)].iter().collect();
        assert_value_equal(
            &udf.invoke_batch(
                &[create_array_value(
                    &[Some("POINT (1 2)"), None, Some("POINT (1 2)")],
                    &sedona_type,
                )],
                1,
            )
            .unwrap(),
            &ColumnarValue::Array(Arc::new(expected_array)),
        );
    }

    #[rstest]
    fn udf_geometry_view_input(
        #[values(WKB_VIEW_GEOMETRY, WKB_VIEW_GEOGRAPHY)] sedona_type: SedonaType,
    ) {
        let udf: ScalarUDF = st_asbinary_udf().into();

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(Some("POINT (1 2)"), &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::BinaryView(Some(POINT12.to_vec()))),
        );

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(None, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::BinaryView(None)),
        );

        let expected_array: BinaryViewArray = [Some(POINT12), None, Some(POINT12)].iter().collect();
        assert_value_equal(
            &udf.invoke_batch(
                &[create_array_value(
                    &[Some("POINT (1 2)"), None, Some("POINT (1 2)")],
                    &sedona_type,
                )],
                1,
            )
            .unwrap(),
            &ColumnarValue::Array(Arc::new(expected_array)),
        );
    }
}
