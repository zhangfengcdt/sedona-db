use std::{str::FromStr, sync::Arc, vec};

use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::scalar::ScalarValue;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::{SedonaType, WKB_GEOGRAPHY, WKB_GEOMETRY},
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};
use wkb::writer::write_geometry;
use wkt::Wkt;

/// ST_GeomFromWKT() UDF implementation
///
/// An implementation of WKT reading using GeoRust's wkt crate.
/// See [`st_geogfromwkt_udf`] for the corresponding geography function.
pub fn st_geomfromwkt_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geomfromwkt",
        vec![Arc::new(STGeoFromWKT {
            out_type: WKB_GEOMETRY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeomFromWKT", "Geometry")),
    )
}

/// ST_GeogFromWKT() UDF implementation
///
/// An implementation of WKT reading using GeoRust's wkt crate.
/// See [`st_geomfromwkt_udf`] for the corresponding geometry function.
pub fn st_geogfromwkt_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geogfromwkt",
        vec![Arc::new(STGeoFromWKT {
            out_type: WKB_GEOGRAPHY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeogFromWKT", "Geography")),
    )
}

fn doc(name: &str, out_type_name: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("Construct a {} from WKT", out_type_name),
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
struct STGeoFromWKT {
    out_type: SedonaType,
}

impl SedonaScalarKernel for STGeoFromWKT {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(vec![ArgMatcher::is_string()], self.out_type.clone());
        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        _: &[SedonaType],
        _: &SedonaType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        match args[0].cast_to(&DataType::Utf8View, None)? {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(maybe_wkt) | ScalarValue::Utf8View(maybe_wkt) => {
                    match maybe_wkt {
                        Some(wkt) => {
                            let mut builder = BinaryBuilder::with_capacity(num_rows, 21);
                            invoke_scalar(&wkt, &mut builder)?;
                            builder.append_value(vec![]);
                            let array = builder.finish();
                            Ok(ScalarValue::Binary(Some(array.value(0).to_vec())).into())
                        }
                        None => Ok(ScalarValue::Binary(None).into()),
                    }
                }
                _ => unreachable!(),
            },
            ColumnarValue::Array(array) => {
                let mut builder = BinaryBuilder::with_capacity(num_rows, 21 * num_rows);
                let utf8_array = as_string_view_array(&array)?;
                let string_view_iter = as_string_view_array(utf8_array)?;
                invoke_iter(string_view_iter, &mut builder)?;
                let new_array = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(new_array)))
            }
        }
    }
}

fn invoke_iter<'a, T>(array: T, builder: &mut BinaryBuilder) -> Result<()>
where
    T: IntoIterator<Item = Option<&'a str>>,
{
    for item in array {
        if let Some(wkt_bytes) = item {
            invoke_scalar(wkt_bytes, builder)?;
            builder.append_value(vec![]);
        } else {
            builder.append_null();
        }
    }

    Ok(())
}

fn invoke_scalar(wkt_bytes: &str, builder: &mut BinaryBuilder) -> Result<()> {
    let geometry: Wkt<f64> = Wkt::from_str(wkt_bytes)
        .map_err(|err| DataFusionError::Internal(format!("WKT parse error: {}", err)))?;

    write_geometry(builder, &geometry, wkb::Endianness::LittleEndian)
        .map_err(|err| DataFusionError::Internal(format!("WKB write error: {}", err)))
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
    use sedona_testing::{
        compare::assert_value_equal,
        create::{create_array_value, create_scalar_value},
    };

    use super::*;

    #[test]
    fn udf_metadata() {
        let geog_from_wkt: ScalarUDF = st_geogfromwkt_udf().into();
        assert_eq!(geog_from_wkt.name(), "st_geogfromwkt");
        assert!(geog_from_wkt.documentation().is_some());

        let geom_from_wkt: ScalarUDF = st_geomfromwkt_udf().into();
        assert_eq!(geom_from_wkt.name(), "st_geomfromwkt");
        assert!(geom_from_wkt.documentation().is_some());
    }

    #[rstest]
    fn udf(#[values(DataType::Utf8, DataType::Utf8View)] data_type: DataType) {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();

        assert_value_equal(
            &udf.invoke_batch(
                &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "POINT (1 2)".to_string(),
                )))],
                1,
            )
            .unwrap(),
            &create_scalar_value(Some("POINT (1 2)"), &WKB_GEOMETRY),
        );

        assert_value_equal(
            &udf.invoke_batch(&[ColumnarValue::Scalar(ScalarValue::Utf8(None))], 1)
                .unwrap(),
            &create_scalar_value(None, &WKB_GEOMETRY),
        );

        let utf8_array: StringArray = [Some("POINT (1 2)"), None, Some("POINT (3 4)")]
            .iter()
            .collect();
        let utf8_value = ColumnarValue::Array(Arc::new(utf8_array))
            .cast_to(&data_type, None)
            .unwrap();
        assert_value_equal(
            &udf.invoke_batch(&[utf8_value], 1).unwrap(),
            &create_array_value(
                &[Some("POINT (1 2)"), None, Some("POINT (3 4)")],
                &WKB_GEOMETRY,
            ),
        );
    }

    #[test]
    fn invalid_wkt() {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();

        let err = udf
            .invoke_batch(
                &[ScalarValue::Utf8(Some("this is not valid wkt".to_string())).into()],
                1,
            )
            .unwrap_err();

        assert!(err.message().starts_with("WKT parse error"));
    }

    #[test]
    fn geog() {
        let udf: ScalarUDF = st_geogfromwkt_udf().into();

        assert_value_equal(
            &udf.invoke_batch(
                &[ScalarValue::Utf8(Some("POINT (1 2)".to_string())).into()],
                1,
            )
            .unwrap(),
            &create_scalar_value(Some("POINT (1 2)"), &WKB_GEOGRAPHY),
        );
    }
}
