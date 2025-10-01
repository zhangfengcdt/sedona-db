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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::Stream;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::config::GpuSpatialJoinConfig;
use crate::gpu_backend::GpuBackend;

/// Stream that executes GPU spatial join
///
/// This stream manages the entire GPU spatial join lifecycle:
/// 1. Initialize GPU context
/// 2. Read left Parquet files
/// 3. Read right Parquet files
/// 4. Execute GPU spatial join
/// 5. Emit result batches
pub struct GpuSpatialJoinStream {
    /// Output schema
    schema: SchemaRef,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Task context for accessing object store
    context: Arc<TaskContext>,

    /// GPU backend for spatial operations
    gpu_backend: Option<GpuBackend>,

    /// Current state of the stream
    state: GpuJoinState,

    /// Result batches to emit
    result_batches: VecDeque<RecordBatch>,

    /// Left side batches (accumulated before GPU transfer)
    left_batches: Vec<RecordBatch>,

    /// Right side batches (accumulated before GPU transfer)
    right_batches: Vec<RecordBatch>,

    /// Current file index being processed
    current_left_file_idx: usize,
    current_right_file_idx: usize,
}

/// State machine for GPU spatial join execution
#[derive(Debug)]
enum GpuJoinState {
    /// Initialize GPU context
    Init,

    /// Reading left Parquet files
    ReadLeftFiles,

    /// Reading right Parquet files
    ReadRightFiles,

    /// Execute GPU spatial join
    ExecuteGpuJoin,

    /// Emit result batches
    EmitResults,

    /// All results emitted, stream complete
    Done,

    /// Error occurred, stream failed
    Failed(String),
}

impl GpuSpatialJoinStream {
    /// Create a new GPU spatial join stream
    pub fn new(
        schema: SchemaRef,
        config: GpuSpatialJoinConfig,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            config,
            context,
            gpu_backend: None,
            state: GpuJoinState::Init,
            result_batches: VecDeque::new(),
            left_batches: Vec::new(),
            right_batches: Vec::new(),
            current_left_file_idx: 0,
            current_right_file_idx: 0,
        })
    }

    /// Poll the stream for next batch
    fn poll_next_impl(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &self.state {
                GpuJoinState::Init => {
                    log::info!("Initializing GPU backend for spatial join");
                    match self.initialize_gpu() {
                        Ok(()) => {
                            log::debug!("GPU backend initialized successfully");
                            self.state = GpuJoinState::ReadLeftFiles;
                        }
                        Err(e) => {
                            if self.config.fallback_to_cpu {
                                log::warn!("GPU initialization failed: {}, falling back to CPU", e);
                                self.state = GpuJoinState::Failed(format!(
                                    "GPU initialization failed (fallback to CPU): {}",
                                    e
                                ));
                                return Poll::Ready(Some(Err(e)));
                            } else {
                                log::error!("GPU initialization failed: {}", e);
                                self.state = GpuJoinState::Failed(e.to_string());
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }

                GpuJoinState::ReadLeftFiles => {
                    log::info!(
                        "Reading left Parquet files ({} files)",
                        self.config.left_parquet_files.len()
                    );

                    match self.read_all_left_files() {
                        Ok(()) => {
                            log::debug!(
                                "Read {} left batches with total {} rows",
                                self.left_batches.len(),
                                self.left_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                            );
                            self.state = GpuJoinState::ReadRightFiles;
                        }
                        Err(e) => {
                            log::error!("Failed to read left Parquet files: {}", e);
                            self.state = GpuJoinState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                GpuJoinState::ReadRightFiles => {
                    log::info!(
                        "Reading right Parquet files ({} files)",
                        self.config.right_parquet_files.len()
                    );

                    match self.read_all_right_files() {
                        Ok(()) => {
                            log::debug!(
                                "Read {} right batches with total {} rows",
                                self.right_batches.len(),
                                self.right_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                            );
                            self.state = GpuJoinState::ExecuteGpuJoin;
                        }
                        Err(e) => {
                            log::error!("Failed to read right Parquet files: {}", e);
                            self.state = GpuJoinState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                GpuJoinState::ExecuteGpuJoin => {
                    log::info!("Executing GPU spatial join");

                    match self.execute_gpu_join() {
                        Ok(()) => {
                            log::info!(
                                "GPU join completed, produced {} result batches",
                                self.result_batches.len()
                            );
                            self.state = GpuJoinState::EmitResults;
                        }
                        Err(e) => {
                            log::error!("GPU spatial join failed: {}", e);
                            self.state = GpuJoinState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                GpuJoinState::EmitResults => {
                    if let Some(batch) = self.result_batches.pop_front() {
                        log::debug!("Emitting result batch with {} rows", batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    log::debug!("All results emitted, stream complete");
                    self.state = GpuJoinState::Done;
                }

                GpuJoinState::Done => {
                    return Poll::Ready(None);
                }

                GpuJoinState::Failed(msg) => {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "GPU spatial join failed: {}",
                        msg
                    )))));
                }
            }
        }
    }

    /// Initialize GPU backend
    fn initialize_gpu(&mut self) -> Result<()> {
        let mut backend = GpuBackend::new(self.config.device_id)
            .map_err(|e| DataFusionError::Execution(format!("GPU backend creation failed: {}", e)))?;
        backend.init()
            .map_err(|e| DataFusionError::Execution(format!("GPU initialization failed: {}", e)))?;
        self.gpu_backend = Some(backend);
        Ok(())
    }

    /// Read all left Parquet files
    fn read_all_left_files(&mut self) -> Result<()> {
        // TODO: Implement async Parquet reading
        // For now, this is a placeholder that will be implemented with actual async I/O
        log::warn!("Parquet file reading not yet fully implemented");

        // Placeholder: Create empty batch if no files
        if self.config.left_parquet_files.is_empty() {
            return Err(DataFusionError::Execution(
                "No left Parquet files specified".into(),
            ));
        }

        Ok(())
    }

    /// Read all right Parquet files
    fn read_all_right_files(&mut self) -> Result<()> {
        // TODO: Implement async Parquet reading
        // For now, this is a placeholder that will be implemented with actual async I/O
        log::warn!("Parquet file reading not yet fully implemented");

        // Placeholder: Create empty batch if no files
        if self.config.right_parquet_files.is_empty() {
            return Err(DataFusionError::Execution(
                "No right Parquet files specified".into(),
            ));
        }

        Ok(())
    }

    /// Execute GPU spatial join
    fn execute_gpu_join(&mut self) -> Result<()> {
        let gpu_backend = self.gpu_backend.as_mut().ok_or_else(|| {
            DataFusionError::Execution("GPU backend not initialized".into())
        })?;

        // Check if we have data to join
        if self.left_batches.is_empty() || self.right_batches.is_empty() {
            log::warn!("No data to join (left: {}, right: {})",
                self.left_batches.len(), self.right_batches.len());
            // Create empty result with correct schema
            let empty_batch = RecordBatch::new_empty(self.schema.clone());
            self.result_batches.push_back(empty_batch);
            return Ok(());
        }

        // For now, process first batch from each side as a proof of concept
        // TODO: Implement batched processing for multiple batches
        let left_batch = &self.left_batches[0];
        let right_batch = &self.right_batches[0];

        log::debug!(
            "Processing GPU join: {} left rows, {} right rows",
            left_batch.num_rows(),
            right_batch.num_rows()
        );

        // Execute GPU spatial join
        let result_batch = gpu_backend.spatial_join(
            left_batch,
            right_batch,
            self.config.left_geom_column.index,
            self.config.right_geom_column.index,
            self.config.predicate.into(),
        ).map_err(|e| {
            if self.config.fallback_to_cpu {
                log::warn!("GPU join failed: {}, should fallback to CPU", e);
            }
            DataFusionError::Execution(format!("GPU spatial join execution failed: {}", e))
        })?;

        log::info!("GPU join produced {} rows", result_batch.num_rows());
        self.result_batches.push_back(result_batch);
        Ok(())
    }
}

impl Stream for GpuSpatialJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for GpuSpatialJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// Convert GpuSpatialPredicate to libgpuspatial SpatialPredicate
impl From<crate::config::GpuSpatialPredicate> for sedona_libgpuspatial::SpatialPredicate {
    fn from(pred: crate::config::GpuSpatialPredicate) -> Self {
        match pred {
            crate::config::GpuSpatialPredicate::Relation(p) => p,
        }
    }
}
