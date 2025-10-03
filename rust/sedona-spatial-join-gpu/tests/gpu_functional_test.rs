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
use arrow::ipc::reader::StreamReader;
use std::fs::File;
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
    let _ = env_logger::builder().is_test(true).try_init();
    
    if !is_gpu_available() {
        eprintln!("GPU not available, skipping test");
        return;
    }

    let test_data_dir = "/home/ubuntu/libspatial/git/sedona-db/c/sedona-libgpuspatial/libgpuspatial/test_data";
    let test_data_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../c/sedona-libgpuspatial/libgpuspatial/test_data");
    let points_path = format!("{}/test_points.arrows", test_data_dir);
    let polygons_path = format!("{}/test_polygons.arrows", test_data_dir);
    
    let points_file = File::open(&points_path)
        .unwrap_or_else(|_| panic!("Failed to open {}", points_path));
    let polygons_file = File::open(&polygons_path)
        .unwrap_or_else(|_| panic!("Failed to open {}", polygons_path));
    
    let mut points_reader = StreamReader::try_new(points_file, None).unwrap();
    let mut polygons_reader = StreamReader::try_new(polygons_file, None).unwrap();
    let points_batch = points_reader.next().unwrap().unwrap();
    let polygons_batch = polygons_reader.next().unwrap().unwrap();
    
    println!("Points batch: {} rows", points_batch.num_rows());
    println!("Polygons batch: {} rows", polygons_batch.num_rows());
    
    // Find geometry column index
    let points_geom_idx = points_batch.schema().index_of("geometry").expect("geometry column not found");
    let polygons_geom_idx = polygons_batch.schema().index_of("geometry").expect("geometry column not found");
    
    // Create execution plans from the batches
    let left_plan = Arc::new(SingleBatchExec::new(polygons_batch.clone())) as Arc<dyn ExecutionPlan>;
    let right_plan = Arc::new(SingleBatchExec::new(points_batch.clone())) as Arc<dyn ExecutionPlan>;
    
    let config = GpuSpatialJoinConfig {
        join_type: datafusion::logical_expr::JoinType::Inner,
        left_geom_column: GeometryColumnInfo {
            name: "geometry".to_string(),
            index: polygons_geom_idx,
        },
        right_geom_column: GeometryColumnInfo {
            name: "geometry".to_string(),
            index: points_geom_idx,
        },
        predicate: GpuSpatialPredicate::Relation(SpatialPredicate::Contains),
        device_id: 0,
        batch_size: 8192,
        additional_filters: None,
        max_memory: None,
        fallback_to_cpu: false,
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

    println!("Total rows from GPU join: {}", total_rows);
    assert!(total_rows > 0, "Expected at least some join results");
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

/// Helper execution plan that returns a single pre-loaded batch
struct SingleBatchExec {
    schema: Arc<Schema>,
    batch: RecordBatch,
    props: datafusion::physical_plan::PlanProperties,
}

impl SingleBatchExec {
    fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        let eq_props = datafusion::physical_expr::EquivalenceProperties::new(schema.clone());
        let partitioning = datafusion::physical_plan::Partitioning::UnknownPartitioning(1);
        let props = datafusion::physical_plan::PlanProperties::new(
            eq_props,
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            schema,
            batch,
            props,
        }
    }
}

impl std::fmt::Debug for SingleBatchExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SingleBatchExec")
    }
}

impl datafusion::physical_plan::DisplayAs for SingleBatchExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "SingleBatchExec")
    }
}

impl datafusion::physical_plan::ExecutionPlan for SingleBatchExec {
    fn name(&self) -> &str {
        "SingleBatchExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
        use futures::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct OnceBatchStream {
            schema: Arc<Schema>,
            batch: Option<RecordBatch>,
        }

        impl Stream for OnceBatchStream {
            type Item = datafusion_common::Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Poll::Ready(self.batch.take().map(Ok))
            }
        }

        impl RecordBatchStream for OnceBatchStream {
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
        }

        Ok(Box::pin(OnceBatchStream {
            schema: self.schema.clone(),
            batch: Some(self.batch.clone()),
        }) as SendableRecordBatchStream)
    }
}
