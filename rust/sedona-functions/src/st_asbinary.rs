use std::{sync::Arc, vec};

use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::SedonaPhysicalType,
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};

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
    fn return_type(&self, args: &[SedonaPhysicalType]) -> Result<Option<SedonaPhysicalType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Binary.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        _: &[SedonaPhysicalType],
        _: &SedonaPhysicalType,
        args: &[ColumnarValue],
        _: usize,
    ) -> Result<ColumnarValue> {
        // This currently works because all current geometry/geography arrays internally
        // use WKB.
        Ok(args[0].clone())
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use sedona_schema::datatypes::WKB_GEOMETRY;

    use crate::st_point::st_point_udf;

    use super::*;

    #[test]
    fn udf() -> Result<()> {
        let st_point: ScalarUDF = st_point_udf().into();
        let wkb_point = st_point.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(Some(2.0)).into(),
            ],
            1,
        )?;

        let udf: ScalarUDF = st_asbinary_udf().into();
        assert_eq!(udf.name(), "st_asbinary");
        let out = udf.invoke_batch(&[wkb_point.clone()], 1)?;

        assert_eq!(
            *out.to_array(1)?,
            *WKB_GEOMETRY.unwrap_arg(&wkb_point)?.to_array(1)?
        );

        Ok(())
    }
}
