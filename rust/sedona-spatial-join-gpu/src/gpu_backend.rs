use crate::Result;
use arrow::compute::take;
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::Schema;
use sedona_libgpuspatial::{GpuSpatialContext, SpatialPredicate};
use std::sync::Arc;

/// GPU backend for spatial operations
#[allow(dead_code)]
pub struct GpuBackend {
    device_id: i32,
    gpu_context: Option<GpuSpatialContext>,
}

#[allow(dead_code)]
impl GpuBackend {
    pub fn new(device_id: i32) -> Result<Self> {
        Ok(Self {
            device_id,
            gpu_context: None,
        })
    }

    pub fn init(&mut self) -> Result<()> {
        // Initialize GPU context
        match GpuSpatialContext::new() {
            Ok(mut ctx) => {
                ctx.init().map_err(|e| {
                    crate::Error::GpuInit(format!("Failed to initialize GPU context: {e:?}"))
                })?;
                self.gpu_context = Some(ctx);
                Ok(())
            }
            Err(e) => {
                log::warn!("GPU not available: {e:?}");
                // Gracefully handle GPU not being available
                Ok(())
            }
        }
    }

    pub fn spatial_join(
        &mut self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        left_geom_col: usize,
        right_geom_col: usize,
        predicate: SpatialPredicate,
    ) -> Result<RecordBatch> {
        let gpu_ctx = match &mut self.gpu_context {
            Some(ctx) => ctx,
            None => {
                return Err(crate::Error::GpuInit(
                    "GPU context not available - falling back to CPU".into(),
                ));
            }
        };

        // Extract geometry columns from both batches
        let left_geom = left_batch.column(left_geom_col);
        let right_geom = right_batch.column(right_geom_col);

        // Perform GPU spatial join
        match gpu_ctx.spatial_join(left_geom.clone(), right_geom.clone(), predicate) {
            Ok((build_indices, stream_indices)) => {
                // Create result record batch from the join indices
                self.create_result_batch(left_batch, right_batch, &build_indices, &stream_indices)
            }
            Err(e) => {
                log::warn!("GPU spatial join failed: {e:?}, falling back to CPU");
                Err(crate::Error::GpuSpatial(format!(
                    "GPU spatial join failed: {e:?}"
                )))
            }
        }
    }

    /// Create result RecordBatch from join indices
    fn create_result_batch(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        build_indices: &[u32],
        stream_indices: &[u32],
    ) -> Result<RecordBatch> {
        if build_indices.len() != stream_indices.len() {
            return Err(crate::Error::GpuSpatial(
                "Mismatched join result lengths".into(),
            ));
        }

        let num_matches = build_indices.len();
        if num_matches == 0 {
            // Return empty result with combined schema
            let combined_schema =
                self.create_combined_schema(&left_batch.schema(), &right_batch.schema())?;
            return Ok(RecordBatch::new_empty(Arc::new(combined_schema)));
        }

        // Build arrays for left side (build indices)
        let mut left_arrays: Vec<ArrayRef> = Vec::new();
        for i in 0..left_batch.num_columns() {
            let column = left_batch.column(i);
            let selected = take(
                column.as_ref(),
                &UInt32Array::from(build_indices.to_vec()),
                None,
            )?;
            left_arrays.push(selected);
        }

        // Build arrays for right side (stream indices)
        let mut right_arrays: Vec<ArrayRef> = Vec::new();
        for i in 0..right_batch.num_columns() {
            let column = right_batch.column(i);
            let selected = take(
                column.as_ref(),
                &UInt32Array::from(stream_indices.to_vec()),
                None,
            )?;
            right_arrays.push(selected);
        }

        // Combine arrays and create schema
        let mut all_arrays = left_arrays;
        all_arrays.extend(right_arrays);

        let combined_schema =
            self.create_combined_schema(&left_batch.schema(), &right_batch.schema())?;

        Ok(RecordBatch::try_new(Arc::new(combined_schema), all_arrays)?)
    }

    /// Create combined schema for join result
    fn create_combined_schema(
        &self,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Schema> {
        // Combine schemas directly without prefixes to match exec.rs schema creation
        let mut fields = left_schema.fields().to_vec();
        fields.extend_from_slice(right_schema.fields());
        Ok(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_backend_creation() {
        let backend = GpuBackend::new(0);
        assert!(backend.is_ok());
    }

    #[test]
    fn test_gpu_backend_initialization() {
        let mut backend = GpuBackend::new(0).unwrap();
        let result = backend.init();

        #[cfg(gpu_available)]
        assert!(result.is_ok());
        #[cfg(not(gpu_available))]
        assert!(result.is_ok()); // Should still succeed but with no GPU context
    }
}
