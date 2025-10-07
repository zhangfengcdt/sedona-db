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
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::stream::Stream;

use crate::config::GpuSpatialJoinConfig;
use crate::gpu_backend::GpuBackend;

/// Stream that executes GPU spatial join
///
/// This stream manages the entire GPU spatial join lifecycle:
/// 1. Initialize GPU context
/// 2. Read data from left child stream
/// 3. Read data from right child stream
/// 4. Execute GPU spatial join
/// 5. Emit result batches
pub struct GpuSpatialJoinStream {
    /// Left child execution plan
    left: Arc<dyn ExecutionPlan>,

    /// Right child execution plan
    right: Arc<dyn ExecutionPlan>,

    /// Output schema
    schema: SchemaRef,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Task context
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

    /// Left child stream
    left_stream: Option<SendableRecordBatchStream>,

    /// Right child stream
    right_stream: Option<SendableRecordBatchStream>,
}

/// State machine for GPU spatial join execution
#[derive(Debug)]
enum GpuJoinState {
    /// Initialize GPU context
    Init,

    /// Initialize left child stream
    InitLeftStream,

    /// Reading batches from left stream
    ReadLeftStream,

    /// Initialize right child stream
    InitRightStream,

    /// Reading batches from right stream
    ReadRightStream,

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
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        config: GpuSpatialJoinConfig,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        Ok(Self {
            left,
            right,
            schema,
            config,
            context,
            gpu_backend: None,
            state: GpuJoinState::Init,
            result_batches: VecDeque::new(),
            left_batches: Vec::new(),
            right_batches: Vec::new(),
            left_stream: None,
            right_stream: None,
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
                            self.state = GpuJoinState::InitLeftStream;
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

                GpuJoinState::InitLeftStream => {
                    log::debug!("Initializing left child stream");
                    match self.left.execute(0, self.context.clone()) {
                        Ok(stream) => {
                            self.left_stream = Some(stream);
                            self.state = GpuJoinState::ReadLeftStream;
                        }
                        Err(e) => {
                            log::error!("Failed to execute left child: {}", e);
                            self.state = GpuJoinState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                GpuJoinState::ReadLeftStream => {
                    if let Some(stream) = &mut self.left_stream {
                        match Pin::new(stream).poll_next(_cx) {
                            Poll::Ready(Some(Ok(batch))) => {
                                log::debug!("Received left batch with {} rows", batch.num_rows());
                                self.left_batches.push(batch);
                                // Continue reading more batches
                                continue;
                            }
                            Poll::Ready(Some(Err(e))) => {
                                log::error!("Error reading left stream: {}", e);
                                self.state = GpuJoinState::Failed(e.to_string());
                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Ready(None) => {
                                // Left stream complete
                                log::debug!(
                                    "Read {} left batches with total {} rows",
                                    self.left_batches.len(),
                                    self.left_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                                );
                                self.state = GpuJoinState::InitRightStream;
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    } else {
                        self.state = GpuJoinState::Failed("Left stream not initialized".into());
                        return Poll::Ready(Some(Err(DataFusionError::Execution(
                            "Left stream not initialized".into(),
                        ))));
                    }
                }

                GpuJoinState::InitRightStream => {
                    log::debug!("Initializing right child stream");
                    match self.right.execute(0, self.context.clone()) {
                        Ok(stream) => {
                            self.right_stream = Some(stream);
                            self.state = GpuJoinState::ReadRightStream;
                        }
                        Err(e) => {
                            log::error!("Failed to execute right child: {}", e);
                            self.state = GpuJoinState::Failed(e.to_string());
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                GpuJoinState::ReadRightStream => {
                    if let Some(stream) = &mut self.right_stream {
                        match Pin::new(stream).poll_next(_cx) {
                            Poll::Ready(Some(Ok(batch))) => {
                                log::debug!("Received right batch with {} rows", batch.num_rows());
                                self.right_batches.push(batch);
                                // Continue reading more batches
                                continue;
                            }
                            Poll::Ready(Some(Err(e))) => {
                                log::error!("Error reading right stream: {}", e);
                                self.state = GpuJoinState::Failed(e.to_string());
                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Ready(None) => {
                                // Right stream complete
                                log::debug!(
                                    "Read {} right batches with total {} rows",
                                    self.right_batches.len(),
                                    self.right_batches.iter().map(|b| b.num_rows()).sum::<usize>()
                                );
                                self.state = GpuJoinState::ExecuteGpuJoin;
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    } else {
                        self.state = GpuJoinState::Failed("Right stream not initialized".into());
                        return Poll::Ready(Some(Err(DataFusionError::Execution(
                            "Right stream not initialized".into(),
                        ))));
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

        log::info!(
            "Processing GPU join with {} left batches and {} right batches",
            self.left_batches.len(),
            self.right_batches.len()
        );

        // Process all combinations of left and right batches
        // This implements a nested loop join at the batch level
        for (left_idx, left_batch) in self.left_batches.iter().enumerate() {
            for (right_idx, right_batch) in self.right_batches.iter().enumerate() {
                log::debug!(
                    "Processing GPU join batch pair ({}, {}): {} left rows, {} right rows",
                    left_idx,
                    right_idx,
                    left_batch.num_rows(),
                    right_batch.num_rows()
                );

                // Execute GPU spatial join for this batch pair
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

                log::debug!("GPU join batch pair ({}, {}) produced {} rows",
                    left_idx, right_idx, result_batch.num_rows());

                // Only add non-empty result batches
                if result_batch.num_rows() > 0 {
                    self.result_batches.push_back(result_batch);
                }
            }
        }

        let total_rows: usize = self.result_batches.iter().map(|b| b.num_rows()).sum();
        log::info!("GPU join produced {} total rows across {} result batches",
            total_rows, self.result_batches.len());

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
