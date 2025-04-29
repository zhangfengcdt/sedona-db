use std::{sync::Arc, vec};

use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF};
use sedona_schema::datatypes::{SedonaType, WKB_GEOMETRY, WKB_VIEW_GEOGRAPHY, WKB_VIEW_GEOMETRY};

use crate::iter_geo_traits;

/// ST_GeomFromWKB() scalar UDF implementation
///
/// An implementation of WKB reading using GeoRust's wkb crate.
pub fn st_geomfromwkb_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geomfromwkb",
        vec![Arc::new(STGeomFromWKB {
            validate: true,
            out_type: WKB_VIEW_GEOMETRY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeomFromWKB", "Geometry")),
    )
}

/// ST_GeogFromWKB() scalar UDF implementation
///
/// An implementation of WKB reading using GeoRust's wkb crate.
pub fn st_geogfromwkb_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geogfromwkb",
        vec![Arc::new(STGeomFromWKB {
            validate: true,
            out_type: WKB_VIEW_GEOGRAPHY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeogFromWKB", "Geography")),
    )
}

fn doc(name: &str, out_type_name: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("Construct a {} from WKB", out_type_name),
        format!("SELECT {name}('POINT(40.7128 -74.0060)')"),
    )
    .with_argument(
        "WKT",
        format!(
            "string: Well-known text representation of the {}",
            out_type_name.to_lowercase()
        ),
    )
    .with_related_udf("ST_AsText")
    .build()
}

#[derive(Debug)]
struct STGeomFromWKB {
    validate: bool,
    out_type: SedonaType,
}

impl SedonaScalarKernel for STGeomFromWKB {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(vec![ArgMatcher::is_binary()], self.out_type.clone());
        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        out_type: &SedonaType,
        args: &[ColumnarValue],
        _num_rows: usize,
    ) -> Result<ColumnarValue> {
        if self.validate {
            let iter_type = match &arg_types[0] {
                SedonaType::Arrow(data_type) => match data_type {
                    DataType::Binary => WKB_GEOMETRY,
                    DataType::BinaryView => WKB_VIEW_GEOGRAPHY,
                    _ => unreachable!(),
                },
                _ => {
                    unreachable!()
                }
            };

            iter_geo_traits!(iter_type, &args[0], |_i, maybe_item| -> Result<()> {
                if let Some(item) = maybe_item {
                    item?;
                }

                Ok(())
            });
        }

        args[0].cast_to(out_type.storage_type(), None)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::BinaryArray;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
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
        let geog_from_wkb: ScalarUDF = st_geogfromwkb_udf().into();
        assert_eq!(geog_from_wkb.name(), "st_geogfromwkb");
        assert!(geog_from_wkb.documentation().is_some());

        let geom_from_wkb: ScalarUDF = st_geomfromwkb_udf().into();
        assert_eq!(geom_from_wkb.name(), "st_geomfromwkb");
        assert!(geom_from_wkb.documentation().is_some());
    }

    #[rstest]
    fn udf(#[values(DataType::Binary, DataType::BinaryView)] data_type: DataType) {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();

        assert_value_equal(
            &udf.invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    POINT12.to_vec(),
                )))],
                1,
            )
            .unwrap(),
            &create_scalar_value(Some("POINT (1 2)"), &WKB_VIEW_GEOMETRY),
        );

        assert_value_equal(
            &udf.invoke_batch(&[ColumnarValue::Scalar(ScalarValue::Binary(None))], 1)
                .unwrap(),
            &create_scalar_value(None, &WKB_VIEW_GEOMETRY),
        );

        let binary_array: BinaryArray = [Some(POINT12), None, Some(POINT12)].iter().collect();
        let binary_value = ColumnarValue::Array(Arc::new(binary_array))
            .cast_to(&data_type, None)
            .unwrap();
        assert_value_equal(
            &udf.invoke_batch(&[binary_value], 1).unwrap(),
            &create_array_value(
                &[Some("POINT (1 2)"), None, Some("POINT (1 2)")],
                &WKB_VIEW_GEOMETRY,
            ),
        );
    }

    #[test]
    fn invalid_wkb() {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();

        let err = udf
            .invoke_batch(&[ScalarValue::Binary(Some(vec![])).into()], 1)
            .unwrap_err();

        assert_eq!(err.message(), "failed to fill whole buffer");
    }

    #[test]
    fn geog() {
        let udf: ScalarUDF = st_geogfromwkb_udf().into();
        let wkb_scalar = ScalarValue::Binary(Some(POINT12.to_vec()));

        assert_value_equal(
            &udf.invoke_batch(&[wkb_scalar.clone().into()], 1).unwrap(),
            &create_scalar_value(Some("POINT (1 2)"), &WKB_VIEW_GEOGRAPHY),
        );
    }
}
