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

//! GPU Functional Tests
//!
//! These tests require actual GPU hardware and CUDA toolkit.
//! They verify the correctness and performance of actual GPU computation.
//!
//! **Prerequisites:**
//! - CUDA-capable GPU (compute capability 6.0+)
//! - CUDA Toolkit 11.0+ installed
//! - Linux or Windows OS
//! - Build with --features gpu
//!
//! **Running:**
//! ```bash
//! # Run all GPU functional tests
//! cargo test --package sedona-spatial-join-gpu --features gpu gpu_functional_tests
//!
//! # Run ignored tests (requires GPU)
//! cargo test --package sedona-spatial-join-gpu --features gpu -- --ignored
//! ```

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int32Array, RecordBatch};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use sedona_spatial_join_gpu::{
    GeometryColumnInfo, GpuSpatialJoinConfig, GpuSpatialJoinExec, GpuSpatialPredicate,
    SpatialPredicate,
};
use std::sync::Arc;

/// Helper to create test geometry data
fn create_point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut wkb = vec![0x01, 0x01, 0x00, 0x00, 0x00]; // Little endian point type
    wkb.extend_from_slice(&x.to_le_bytes());
    wkb.extend_from_slice(&y.to_le_bytes());
    wkb
}

/// Check if GPU is actually available
fn is_gpu_available() -> bool {
    use sedona_libgpuspatial::GpuSpatialContext;

    match GpuSpatialContext::new() {
        Ok(mut ctx) => ctx.init().is_ok(),
        Err(_) => false,
    }
}

/// Mock execution plan that produces geometry data
struct GeometryDataExec {
    schema: Arc<Schema>,
    batch: RecordBatch,
}

impl GeometryDataExec {
    fn new(ids: Vec<i32>, geometries: Vec<Vec<u8>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let id_array = Int32Array::from(ids);

        // Build BinaryArray using builder to avoid lifetime issues
        let mut builder = arrow_array::builder::BinaryBuilder::new();
        for geom in geometries {
            builder.append_value(&geom);
        }
        let geom_array = builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(geom_array)],
        )
        .unwrap();

        Self { schema, batch }
    }
}

impl std::fmt::Debug for GeometryDataExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeometryDataExec")
    }
}

impl datafusion::physical_plan::DisplayAs for GeometryDataExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "GeometryDataExec")
    }
}

impl ExecutionPlan for GeometryDataExec {
    fn name(&self) -> &str {
        "GeometryDataExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        unimplemented!("properties not needed for test")
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
        use futures::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct SingleBatchStream {
            schema: Arc<Schema>,
            batch: Option<RecordBatch>,
        }

        impl Stream for SingleBatchStream {
            type Item = datafusion_common::Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Poll::Ready(self.batch.take().map(Ok))
            }
        }

        impl RecordBatchStream for SingleBatchStream {
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
        }

        Ok(Box::pin(SingleBatchStream {
            schema: self.schema.clone(),
            batch: Some(self.batch.clone()),
        }) as SendableRecordBatchStream)
    }
}

#[tokio::test]
#[ignore] // Requires GPU hardware
async fn test_gpu_spatial_join_basic_correctness() {
    if !is_gpu_available() {
        eprintln!("GPU not available, skipping test");
        return;
    }

    // Create left dataset: 3 points at (0,0), (1,1), (2,2)
    let left_ids = vec![1, 2, 3];
    let left_geoms = vec![
        create_point_wkb(0.0, 0.0),
        create_point_wkb(1.0, 1.0),
        create_point_wkb(2.0, 2.0),
    ];
    let left_plan = Arc::new(GeometryDataExec::new(left_ids, left_geoms)) as Arc<dyn ExecutionPlan>;

    // Create right dataset: 3 points at (0,0), (1,1), (5,5)
    let right_ids = vec![10, 11, 12];
    let right_geoms = vec![
        create_point_wkb(0.0, 0.0),
        create_point_wkb(1.0, 1.0),
        create_point_wkb(5.0, 5.0),
    ];
    let right_plan = Arc::new(GeometryDataExec::new(right_ids, right_geoms)) as Arc<dyn ExecutionPlan>;

    let config = GpuSpatialJoinConfig {
        join_type: datafusion::logical_expr::JoinType::Inner,
        left_geom_column: GeometryColumnInfo {
            name: "geometry".to_string(),
            index: 1,
        },
        right_geom_column: GeometryColumnInfo {
            name: "geometry".to_string(),
            index: 1,
        },
        predicate: GpuSpatialPredicate::Relation(SpatialPredicate::Intersects),
        device_id: 0,
        batch_size: 8192,
        additional_filters: None,
        max_memory: None,
        fallback_to_cpu: false, // Must use GPU for this test
    };

    let gpu_join = Arc::new(GpuSpatialJoinExec::new(left_plan, right_plan, config).unwrap());

    let task_context = Arc::new(TaskContext::default());
    let mut stream = gpu_join.execute(0, task_context).unwrap();

    let mut total_rows = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => {
                total_rows += batch.num_rows();
                println!("Got {} rows from GPU join", batch.num_rows());
            }
            Err(e) => {
                panic!("GPU join failed: {}", e);
            }
        }
    }

    // Expected: 3 matches (point intersects itself)
    // (0,0) with (0,0), (1,1) with (1,1), (2,2) does not match (5,5)
    assert!(
        total_rows >= 2,
        "Expected at least 2 intersecting point pairs, got {}",
        total_rows
    );
}

#[tokio::test]
#[ignore] // Requires GPU hardware
async fn test_gpu_vs_cpu_consistency() {
    if !is_gpu_available() {
        eprintln!("GPU not available, skipping test");
        return;
    }

    // TODO: Implement test that compares GPU results with CPU spatial join
    // This would verify correctness by comparing against known-good CPU implementation

    println!("GPU vs CPU consistency test not yet implemented");
    println!("This test should:");
    println!("  1. Run same query on GPU");
    println!("  2. Run same query on CPU");
    println!("  3. Compare results for equality");
}

#[tokio::test]
#[ignore] // Requires GPU hardware
async fn test_gpu_performance_baseline() {
    if !is_gpu_available() {
        eprintln!("GPU not available, skipping test");
        return;
    }

    // TODO: Implement performance baseline test
    // This would measure GPU performance on standard datasets

    println!("GPU performance baseline test not yet implemented");
    println!("This test should:");
    println!("  1. Create dataset of known size (e.g., 100K points)");
    println!("  2. Measure GPU join time");
    println!("  3. Verify reasonable performance (e.g., < 1s for 100K)");
}
