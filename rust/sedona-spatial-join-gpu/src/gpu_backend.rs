use crate::Result;
use arrow::compute::take;
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Schema};
use std::sync::Arc;
use sedona_libgpuspatial::{GpuSpatialContext, SpatialPredicate};

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

    /// Convert BinaryView array to Binary array for GPU processing
    fn ensure_binary_array(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::BinaryView => {
                // Convert BinaryView to Binary for GPU processing
                use arrow_array::cast::AsArray;
                let binary_view = array.as_binary_view();

                // Log first few values for debugging
                if binary_view.len() > 0 {
                    eprintln!("DEBUG: Converting BinaryView with {} rows", binary_view.len());
                    for i in 0..binary_view.len().min(3) {
                        if !binary_view.is_null(i) {
                            let val = binary_view.value(i);
                            eprintln!("  Row {}: {} bytes, first 20: {:?}", i, val.len(), &val[..val.len().min(20)]);
                        }
                    }
                }

                let binary_array = BinaryArray::from_iter(
                    (0..binary_view.len()).map(|i| {
                        if binary_view.is_null(i) {
                            None
                        } else {
                            Some(binary_view.value(i))
                        }
                    })
                );

                // Verify conversion
                if binary_array.len() > 0 {
                    eprintln!("DEBUG: Converted to Binary with {} rows", binary_array.len());
                    for i in 0..binary_array.len().min(3) {
                        if !binary_array.is_null(i) {
                            let val = binary_array.value(i);
                            eprintln!("  Row {}: {} bytes, first 20: {:?}", i, val.len(), &val[..val.len().min(20)]);
                        }
                    }
                }

                Ok(Arc::new(binary_array) as ArrayRef)
            }
            DataType::Binary | DataType::LargeBinary => {
                // Already in correct format
                Ok(array.clone())
            }
            _ => Err(crate::Error::GpuSpatial(format!(
                "Expected Binary/BinaryView array, got {:?}",
                array.data_type()
            ))),
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

        log::info!(
            "GPU spatial join: left_batch={} rows, right_batch={} rows, left_geom type={:?}, right_geom type={:?}",
            left_batch.num_rows(),
            right_batch.num_rows(),
            left_geom.data_type(),
            right_geom.data_type()
        );

        // Convert BinaryView to Binary if needed
        let left_geom = Self::ensure_binary_array(left_geom)?;
        let right_geom = Self::ensure_binary_array(right_geom)?;

        log::info!(
            "After conversion: left_geom type={:?} len={}, right_geom type={:?} len={}",
            left_geom.data_type(),
            left_geom.len(),
            right_geom.data_type(),
            right_geom.len()
        );

        // Debug: Print raw binary data before sending to GPU
        eprintln!("\n=== DETAILED BINARY DUMP BEFORE GPU ===");
        eprintln!("LEFT (build) side: {} geometries", left_geom.len());
        if let Some(left_binary) = left_geom.as_any().downcast_ref::<BinaryArray>() {
            for i in 0..left_binary.len().min(5) {
                if !left_binary.is_null(i) {
                    let wkb = left_binary.value(i);
                    eprintln!("  Left[{}]: {} bytes", i, wkb.len());
                    eprintln!("    Full WKB: {:02x?}", wkb);
                    // Parse WKB header
                    if wkb.len() >= 5 {
                        let byte_order = wkb[0];
                        let geom_type = u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]);
                        eprintln!("    Byte order: {} (1=little endian), Geom type: {}", byte_order, geom_type);
                    }
                }
            }
        }

        eprintln!("\nRIGHT (stream) side: {} geometries", right_geom.len());
        if let Some(right_binary) = right_geom.as_any().downcast_ref::<BinaryArray>() {
            for i in 0..right_binary.len().min(5) {
                if !right_binary.is_null(i) {
                    let wkb = right_binary.value(i);
                    eprintln!("  Right[{}]: {} bytes", i, wkb.len());
                    eprintln!("    Full WKB: {:02x?}", wkb);
                    // Parse WKB header
                    if wkb.len() >= 5 {
                        let byte_order = wkb[0];
                        let geom_type = u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]);
                        eprintln!("    Byte order: {} (1=little endian), Geom type: {}", byte_order, geom_type);
                    }
                }
            }
        }
        eprintln!("=== END BINARY DUMP ===\n");

        // Perform GPU spatial join
        match gpu_ctx.spatial_join(left_geom.clone(), right_geom.clone(), predicate) {
            Ok((build_indices, stream_indices)) => {
                eprintln!("DEBUG: GPU join succeeded: {} matches found", build_indices.len());
                eprintln!("DEBUG: build_indices: {:?}", &build_indices[..build_indices.len().min(10)]);
                eprintln!("DEBUG: stream_indices: {:?}", &stream_indices[..stream_indices.len().min(10)]);

                // Create result record batch from the join indices
                self.create_result_batch(left_batch, right_batch, &build_indices, &stream_indices)
            }
            Err(e) => {
                eprintln!("DEBUG: GPU spatial join failed: {e:?}");
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
