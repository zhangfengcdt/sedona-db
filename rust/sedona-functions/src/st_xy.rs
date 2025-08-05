use std::{sync::Arc, vec};

use crate::executor::WkbExecutor;
use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use sedona_expr::scalar_udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF};
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

/// ST_X() scalar UDF implementation
///
/// Extract the X coordinate from Point geometries or geographies
pub fn st_x_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_x",
        vec![Arc::new(STXy { dim: "x" })],
        Volatility::Immutable,
        Some(st_xy_doc("x")),
    )
}

/// ST_Y() scalar UDF implementation
///
/// Extract the Y coordinate from point geometries or geographies
pub fn st_y_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_y",
        vec![Arc::new(STXy { dim: "y" })],
        Volatility::Immutable,
        Some(st_xy_doc("y")),
    )
}

fn st_xy_doc(dim: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!(
            "Return the {} component of a point geometry or geography",
            dim.to_uppercase()
        ),
        format!("ST_{}(A: Point)", dim.to_uppercase()),
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .with_sql_example(format!(
        "SELECT ST_{}(ST_Point(1.0, 2.0))",
        dim.to_uppercase()
    ))
    .build()
}

#[derive(Debug)]
struct STXy {
    dim: &'static str,
}

impl SedonaScalarKernel for STXy {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Float64.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let dim_index = match self.dim {
            "x" => 0,
            "y" => 1,
            _ => unreachable!(),
        };

        let executor = WkbExecutor::new(arg_types, args);
        let mut builder = Float64Builder::with_capacity(executor.num_iterations());

        // We can do quite a lot better than this with some vectorized WKB processing,
        // but for now we just do a slow iteration
        executor.execute_wkb_void(|maybe_item| {
            match maybe_item {
                Some(item) => {
                    builder.append_option(invoke_scalar(&item, dim_index)?);
                }
                None => builder.append_null(),
            }
            Ok(())
        })?;

        // Create the output array
        executor.finish(Arc::new(builder.finish()))
    }
}

// Extracts the 0th or 1st dimension from any point-like or EMPTY GeometryTrait
//
// Note that PostGIS will fail for anything that is not POINT (whereas we succeed for any
// EMPTY).
fn invoke_scalar(item: &Wkb, dim_index: usize) -> Result<Option<f64>> {
    match item.as_type() {
        geo_traits::GeometryType::Point(point) => {
            return Ok(PointTrait::coord(point).map(|c| c.nth_or_panic(dim_index)))
        }
        geo_traits::GeometryType::LineString(linestring) => {
            if LineStringTrait::num_coords(linestring) == 0 {
                return Ok(None);
            }
        }
        geo_traits::GeometryType::Polygon(polygon) => {
            if PolygonTrait::exterior(polygon).is_none() {
                return Ok(None);
            }
        }
        geo_traits::GeometryType::MultiPoint(multipoint) => {
            match MultiPointTrait::num_points(multipoint) {
                0 => return Ok(None),
                1 => {
                    return Ok(
                        PointTrait::coord(&MultiPointTrait::point(multipoint, 0).unwrap())
                            .map(|c| c.nth_or_panic(dim_index)),
                    )
                }
                _ => {}
            }
        }
        geo_traits::GeometryType::MultiLineString(multilinestring) => {
            if MultiLineStringTrait::num_line_strings(multilinestring) == 0 {
                return Ok(None);
            }
        }
        geo_traits::GeometryType::MultiPolygon(multipolygon) => {
            if MultiPolygonTrait::num_polygons(multipolygon) == 0 {
                return Ok(None);
            }
        }
        geo_traits::GeometryType::GeometryCollection(geometrycollection) => {
            if geometrycollection.num_geometries() == 0 {
                return Ok(None);
            }
        }
        _ => {}
    };

    Err(DataFusionError::Execution("Expected POINT".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{create_array, ArrayRef};
    use datafusion_common::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
    use sedona_schema::datatypes::{
        WKB_GEOGRAPHY, WKB_GEOMETRY, WKB_VIEW_GEOGRAPHY, WKB_VIEW_GEOMETRY,
    };
    use sedona_testing::{
        create::create_array, fixtures::MULTIPOINT_WITH_EMPTY_CHILD_WKB, testers::ScalarUdfTester,
    };

    #[test]
    fn udf_metadata() {
        let udf_x: ScalarUDF = st_x_udf().into();
        assert_eq!(udf_x.name(), "st_x");
        assert!(udf_x.documentation().is_some());

        let udf_y: ScalarUDF = st_y_udf().into();
        assert_eq!(udf_y.name(), "st_y");
        assert!(udf_y.documentation().is_some());
    }

    #[rstest]
    fn udf_invoke(
        #[values(WKB_GEOMETRY, WKB_GEOGRAPHY, WKB_VIEW_GEOMETRY, WKB_VIEW_GEOGRAPHY)]
        sedona_type: SedonaType,
    ) {
        let x_tester = ScalarUdfTester::new(st_x_udf().into(), vec![sedona_type.clone()]);
        let y_tester = ScalarUdfTester::new(st_y_udf().into(), vec![sedona_type.clone()]);

        assert_eq!(
            x_tester.return_type().unwrap(),
            SedonaType::Arrow(DataType::Float64)
        );
        assert_eq!(
            y_tester.return_type().unwrap(),
            SedonaType::Arrow(DataType::Float64)
        );

        assert_eq!(
            x_tester.invoke_wkb_scalar(Some("POINT (1 2)")).unwrap(),
            ScalarValue::Float64(Some(1.0))
        );
        assert_eq!(
            y_tester.invoke_wkb_scalar(Some("POINT (1 2)")).unwrap(),
            ScalarValue::Float64(Some(2.0))
        );

        let wkb_array = create_array(
            &[Some("POINT (1 2)"), None, Some("MULTIPOINT (3 4)")],
            &WKB_GEOMETRY,
        );
        let expected_x: ArrayRef = create_array!(Float64, [Some(1.0), None, Some(3.0)]);
        let expected_y: ArrayRef = create_array!(Float64, [Some(2.0), None, Some(4.0)]);
        assert_eq!(
            &x_tester.invoke_array(wkb_array.clone()).unwrap(),
            &expected_x
        );
        assert_eq!(
            &y_tester.invoke_array(wkb_array.clone()).unwrap(),
            &expected_y
        );
    }

    #[rstest]
    fn udf_empties(
        #[values(
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
            "GEOMETRYCOLLECTION"
        )]
        geom_type: &str,
    ) {
        let x_tester = ScalarUdfTester::new(st_x_udf().into(), vec![WKB_GEOMETRY]);
        let y_tester = ScalarUdfTester::new(st_y_udf().into(), vec![WKB_GEOMETRY]);

        let wkt_empty = format!("{geom_type} EMPTY");
        assert_eq!(
            x_tester.invoke_wkb_scalar(Some(&wkt_empty)).unwrap(),
            ScalarValue::Float64(None)
        );
        assert_eq!(
            y_tester.invoke_wkb_scalar(Some(&wkt_empty)).unwrap(),
            ScalarValue::Float64(None)
        );
    }

    #[rstest]
    fn wrong_geometry_type(
        #[values(
            "LINESTRING (0 1, 2 3)",
            "POLYGON ((0 0, 0 1, 1 0, 0 0))",
            "MULTIPOINT ((0 1), (2 3))",
            "MULTILINESTRING ((0 1, 2 3))",
            "MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))",
            "GEOMETRYCOLLECTION (POINT (0 1))"
        )]
        bad_wkt: &str,
    ) {
        let x_tester = ScalarUdfTester::new(st_x_udf().into(), vec![WKB_GEOMETRY]);

        let err = x_tester.invoke_wkb_scalar(Some(bad_wkt)).unwrap_err();
        assert_eq!(err.message(), "Expected POINT");
    }

    #[test]
    fn multipoint_with_empty_child() {
        let x_tester = ScalarUdfTester::new(st_x_udf().into(), vec![WKB_GEOMETRY]);
        let y_tester = ScalarUdfTester::new(st_y_udf().into(), vec![WKB_GEOMETRY]);

        let scalar = WKB_GEOMETRY
            .wrap_scalar(&ScalarValue::Binary(Some(
                MULTIPOINT_WITH_EMPTY_CHILD_WKB.to_vec(),
            )))
            .unwrap();
        assert_eq!(
            x_tester.invoke_scalar(scalar.clone()).unwrap(),
            ScalarValue::Float64(None)
        );
        assert_eq!(
            y_tester.invoke_scalar(scalar.clone()).unwrap(),
            ScalarValue::Float64(None)
        );
    }
}
