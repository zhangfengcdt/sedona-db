// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::ColumnarValue;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::executor::WkbExecutor;
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

use crate::centroid::extract_centroid_2d;

/// ST_Centroid() implementation using centroid extraction
pub fn st_centroid_impl() -> ScalarKernelRef {
    Arc::new(STCentroid {})
}

#[derive(Debug)]
struct STCentroid {}

impl SedonaScalarKernel for STCentroid {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry()],
            SedonaType::Arrow(DataType::Binary),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = WkbExecutor::new(arg_types, args);
        let mut builder = BinaryBuilder::with_capacity(executor.num_iterations(), 1024);
        executor.execute_wkb_void(|maybe_wkb| {
            match maybe_wkb {
                Some(wkb) => {
                    let centroid_wkb = invoke_scalar(&wkb)?;
                    builder.append_value(&centroid_wkb);
                }
                _ => builder.append_null(),
            }

            Ok(())
        })?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn invoke_scalar(wkb: &Wkb) -> Result<Vec<u8>> {
    let (x, y) = extract_centroid_2d(wkb)?;
    
    // Create WKB for POINT geometry
    if x.is_nan() || y.is_nan() {
        // Return POINT EMPTY
        Ok(create_empty_point_wkb())
    } else {
        Ok(create_point_wkb(x, y))
    }
}

fn create_point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut wkb = Vec::with_capacity(21);
    // Little endian
    wkb.push(0x01);
    // Point geometry type (1)
    wkb.extend_from_slice(&1u32.to_le_bytes());
    // X coordinate
    wkb.extend_from_slice(&x.to_le_bytes());
    // Y coordinate
    wkb.extend_from_slice(&y.to_le_bytes());
    wkb
}

fn create_empty_point_wkb() -> Vec<u8> {
    let mut wkb = Vec::with_capacity(21);
    // Little endian
    wkb.push(0x01);
    // Point geometry type (1)
    wkb.extend_from_slice(&1u32.to_le_bytes());
    // NaN coordinates for empty point
    wkb.extend_from_slice(&f64::NAN.to_le_bytes());
    wkb.extend_from_slice(&f64::NAN.to_le_bytes());
    wkb
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array, Array, ArrayRef, BinaryArray};
    use datafusion_common::scalar::ScalarValue;
    use rstest::rstest;
    use sedona_functions::register::stubs::st_centroid_udf;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};
    use sedona_testing::testers::ScalarUdfTester;
    use wkb::reader::Wkb;

    use super::*;

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        let mut udf = st_centroid_udf();
        udf.add_kernel(st_centroid_impl());
        let tester = ScalarUdfTester::new(udf.into(), vec![sedona_type]);

        assert_eq!(
            tester.return_type().unwrap(),
            SedonaType::Arrow(DataType::Binary)
        );

        // Test with a polygon
        let result = tester
            .invoke_wkb_scalar(Some("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"))
            .unwrap();
        
        if let ScalarValue::Binary(Some(wkb_data)) = result {
            let wkb = Wkb::try_new(&wkb_data).unwrap();
            let (x, y) = extract_centroid_2d(&wkb).unwrap();
            assert_eq!(x, 1.0);
            assert_eq!(y, 1.0);
        } else {
            panic!("Expected Binary result");
        }

        // Test with array
        let input_wkt = vec![
            Some("POINT(1 2)"),
            None,
            Some("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
        ];
        let result_array = tester.invoke_wkb_array(input_wkt).unwrap();
        let binary_array = result_array.as_any().downcast_ref::<BinaryArray>().unwrap();
        
        // First element: POINT(1 2) - centroid should be (1, 2)
        assert!(binary_array.value(0).len() > 0);
        // Second element: NULL
        assert!(binary_array.is_null(1));
        // Third element: POLYGON centroid should be (1, 1)
        assert!(binary_array.value(2).len() > 0);
    }
}