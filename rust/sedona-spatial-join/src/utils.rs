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

/// Most of the code in this module are copied from the `datafusion_physical_plan::joins::utils` module.
/// https://github.com/apache/datafusion/blob/48.0.0/datafusion/physical-plan/src/joins/utils.rs
/// We made some slight modification to reference a collection of batches in the build side instead
/// of one giant concatenated batch.
use std::{ops::Range, sync::Arc};

use arrow::array::{new_null_array, Array, BooleanBufferBuilder, RecordBatch, RecordBatchOptions};
use arrow::compute;
use arrow::datatypes::Schema;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{JoinSide, Result};
use datafusion_expr::JoinType;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::execution_plan::Boundedness;
use datafusion_physical_plan::joins::utils::{
    adjust_right_output_partitioning, ColumnIndex, JoinFilter,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// The indices of the rows from the build and probe sides in the joined result.
#[derive(Clone, Debug)]
pub struct JoinedRowsIndices {
    /// The indices of the rows in the build side.
    /// The first element is the batch index in the spatial index, the second element is
    /// the row index in the batch.
    pub build: Vec<(i32, i32)>,
    /// The indices of the rows in the probe side.
    pub probe: Vec<u32>,
}

impl JoinedRowsIndices {
    /// Creates a new JoinedRowsIndices with the given build and probe indices.
    pub fn new(build: Vec<(i32, i32)>, probe: Vec<u32>) -> Self {
        debug_assert_eq!(
            build.len(),
            probe.len(),
            "Build and probe indices must have the same length"
        );
        Self { build, probe }
    }

    /// Creates an empty JoinedRowsIndices with no rows.
    pub fn empty() -> Self {
        Self {
            build: Vec::new(),
            probe: Vec::new(),
        }
    }

    /// Checks if this JoinedRowsIndices is empty (has no rows).
    pub fn is_empty(&self) -> bool {
        self.build.is_empty() && self.probe.is_empty()
    }
}

pub(crate) fn apply_join_filter_to_indices(
    build_batches: &[&RecordBatch],
    joined_indices: &JoinedRowsIndices,
    probe_batch: &RecordBatch,
    filter: &JoinFilter,
    build_side: JoinSide,
) -> Result<JoinedRowsIndices> {
    if joined_indices.is_empty() {
        return Ok(joined_indices.clone());
    }

    // Create intermediate batch for filter evaluation
    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_batches,
        &joined_indices.build,
        probe_batch,
        &joined_indices.probe,
        filter.column_indices(),
        build_side,
    )?;

    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;

    // Filter the positions and indices based on the mask
    let mut filtered_build_positions = Vec::with_capacity(mask.len());
    let mut filtered_probe_indices = Vec::with_capacity(mask.len());

    for i in 0..mask.len() {
        if mask.value(i) {
            filtered_build_positions.push(joined_indices.build[i]);
            filtered_probe_indices.push(joined_indices.probe[i]);
        }
    }

    Ok(JoinedRowsIndices {
        build: filtered_build_positions,
        probe: filtered_probe_indices,
    })
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
pub(crate) fn build_batch_from_indices(
    schema: &Schema,
    build_batches: &[&RecordBatch],
    build_batch_positions: &[(i32, i32)], // (batch_idx, row_idx) pairs
    probe_batch: &RecordBatch,
    probe_indices: &[u32], // probe row indices
    column_indices: &[ColumnIndex],
    build_side: JoinSide,
) -> Result<RecordBatch> {
    let num_rows = build_batch_positions.len();
    debug_assert_eq!(num_rows, probe_indices.len());

    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(num_rows));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == JoinSide::None {
            // LeftMark join, the mark column is a true if the indices is not null, otherwise it will be false
            // For now, assume all probe indices are valid (not null)
            Arc::new(arrow::array::BooleanArray::from(vec![true; num_rows]))
        } else if column_index.side == build_side {
            // Build side - use interleave to efficiently gather from multiple batches
            if build_batch_positions.is_empty() {
                // Shouldn't happen, but handle gracefully
                new_null_array(&arrow::datatypes::DataType::Int32, num_rows)
            } else {
                // Find unique batch indices that we actually need
                let mut needed_batches = std::collections::HashSet::new();
                for &(batch_idx, _) in build_batch_positions {
                    needed_batches.insert(batch_idx as usize);
                }

                // Create a mapping from original batch_idx to array position
                let mut batch_idx_to_array_idx = std::collections::HashMap::new();
                let mut arrays = Vec::with_capacity(needed_batches.len());

                for &batch_idx in &needed_batches {
                    batch_idx_to_array_idx.insert(batch_idx, arrays.len());
                    arrays.push(build_batches[batch_idx].column(column_index.index).as_ref());
                }

                // Create indices for interleave: (array_index_in_arrays, row_index) pairs
                let indices: Vec<(usize, usize)> = build_batch_positions
                    .iter()
                    .map(|&(batch_idx, row_idx)| {
                        let array_idx = batch_idx_to_array_idx[&(batch_idx as usize)];
                        (array_idx, row_idx as usize)
                    })
                    .collect();

                // Use interleave to efficiently gather values
                arrow::compute::interleave(&arrays, &indices)?
            }
        } else {
            // Probe side
            let array = probe_batch.column(column_index.index);
            if array.is_empty() {
                new_null_array(array.data_type(), num_rows)
            } else {
                let indices_array = arrow::array::UInt32Array::from(probe_indices.to_vec());
                compute::take(array.as_ref(), &indices_array, None)?
            }
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub(crate) fn adjust_indices_by_join_type(
    joined_indices: &JoinedRowsIndices,
    adjust_range: Range<usize>,
    join_type: JoinType,
    preserve_order_for_right: bool,
) -> Result<JoinedRowsIndices> {
    match join_type {
        JoinType::Inner => {
            // matched
            Ok(joined_indices.clone())
        }
        JoinType::Left => {
            // matched
            Ok(joined_indices.clone())
            // unmatched left row will be produced in the end of loop, and it has been set in the left visited bitmap
        }
        JoinType::Right => {
            // combine the matched and unmatched right result together
            append_right_indices(joined_indices, adjust_range, preserve_order_for_right)
        }
        JoinType::Full => append_right_indices(joined_indices, adjust_range, false),
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(adjust_range, &joined_indices.probe);
            // the left_indices will not be used later for the `right semi` join
            Ok(JoinedRowsIndices {
                build: joined_indices.build.clone(),
                probe: right_indices,
            })
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(adjust_range, &joined_indices.probe);
            // the left_indices will not be used later for the `right anti` join
            Ok(JoinedRowsIndices {
                build: joined_indices.build.clone(),
                probe: right_indices,
            })
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            Ok(JoinedRowsIndices::empty())
        }
    }
}

/// Appends right indices to left indices based on the specified order mode.
///
/// The function operates in two modes:
/// 1. If `preserve_order_for_right` is true, probe matched and unmatched indices
///    are inserted in order using the `append_probe_indices_in_order()` method.
/// 2. Otherwise, unmatched probe indices are simply appended after matched ones.
///
/// # Parameters
/// - `left_indices`: UInt64Array of left indices.
/// - `right_indices`: UInt32Array of right indices.
/// - `adjust_range`: Range to adjust the right indices.
/// - `preserve_order_for_right`: Boolean flag to determine the mode of operation.
///
/// # Returns
/// A tuple of updated `UInt64Array` and `UInt32Array`.
pub(crate) fn append_right_indices(
    joined_indices: &JoinedRowsIndices,
    adjust_range: Range<usize>,
    preserve_order_for_right: bool,
) -> Result<JoinedRowsIndices> {
    if preserve_order_for_right {
        let (build, probe) = append_probe_indices_in_order(
            &joined_indices.build,
            &joined_indices.probe,
            adjust_range,
        );
        Ok(JoinedRowsIndices { build, probe })
    } else {
        let right_unmatched_indices = get_anti_indices(adjust_range, &joined_indices.probe);

        if right_unmatched_indices.is_empty() {
            Ok(joined_indices.clone())
        } else {
            // `into_builder()` can fail here when there is nothing to be filtered and
            // left_indices or right_indices has the same reference to the cached indices.
            // In that case, we use a slower alternative.

            // the new left indices: left_indices + null placeholders for unmatched right
            let mut new_left_indices =
                Vec::with_capacity(joined_indices.build.len() + right_unmatched_indices.len());
            new_left_indices.extend_from_slice(&joined_indices.build);
            new_left_indices.extend(std::iter::repeat_n((-1, -1), right_unmatched_indices.len()));

            // the new right indices: right_indices + right_unmatched_indices
            let mut new_right_indices =
                Vec::with_capacity(joined_indices.probe.len() + right_unmatched_indices.len());
            new_right_indices.extend_from_slice(&joined_indices.probe);
            new_right_indices.extend_from_slice(&right_unmatched_indices);

            Ok(JoinedRowsIndices {
                build: new_left_indices,
                probe: new_right_indices,
            })
        }
    }
}

/// Returns `range` indices which are not present in `input_indices`
pub(crate) fn get_anti_indices(range: Range<usize>, input_indices: &[u32]) -> Vec<u32> {
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .map(|&v| v as usize)
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| (!bitmap.get_bit(idx - offset)).then_some(idx as u32))
        .collect()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub(crate) fn get_semi_indices(range: Range<usize>, input_indices: &[u32]) -> Vec<u32> {
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .map(|&v| v as usize)
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the semi index
    (range)
        .filter_map(|idx| (bitmap.get_bit(idx - offset)).then_some(idx as u32))
        .collect()
}

/// Appends probe indices in order by considering the given build indices.
///
/// This function constructs new build and probe indices by iterating through
/// the provided indices, and appends any missing values between previous and
/// current probe index with a corresponding null build index.
///
/// # Parameters
///
/// - `build_indices`: `PrimitiveArray` of `UInt64Type` containing build indices.
/// - `probe_indices`: `PrimitiveArray` of `UInt32Type` containing probe indices.
/// - `range`: The range of indices to consider.
///
/// # Returns
///
/// A tuple of two arrays:
/// - A `PrimitiveArray` of `UInt64Type` with the newly constructed build indices.
/// - A `PrimitiveArray` of `UInt32Type` with the newly constructed probe indices.
fn append_probe_indices_in_order(
    left_indices: &[(i32, i32)], // (batch_idx, row_idx) pairs
    right_indices: &[u32],       // probe row indices
    range: Range<usize>,
) -> (Vec<(i32, i32)>, Vec<u32>) {
    // Builders for new indices:
    let mut new_left_indices = Vec::with_capacity(range.len());
    let mut new_right_indices = Vec::with_capacity(range.len());
    // Set previous index as the start index for the initial loop:
    let mut prev_index = range.start as u32;
    // Zip the two iterators.
    debug_assert!(left_indices.len() == right_indices.len());
    for (i, &right_index) in right_indices.iter().enumerate() {
        // Append values between previous and current probe index with null build index:
        for value in prev_index..right_index {
            new_right_indices.push(value);
            new_left_indices.push((-1, -1)); // Placeholder for null build index
        }
        // Append current indices:
        new_right_indices.push(right_index);
        new_left_indices.push(left_indices[i]); // Use actual left index
                                                // Set current probe index as previous for the next iteration:
        prev_index = right_index + 1;
    }
    // Append remaining probe indices after the last valid probe index with null build index.
    for value in prev_index..range.end as u32 {
        new_right_indices.push(value);
        new_left_indices.push((-1, -1)); // Placeholder for null build index
    }
    // Build arrays and return:
    (new_left_indices.into_iter().collect(), new_right_indices)
}

pub(crate) fn asymmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
) -> Partitioning {
    match join_type {
        JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
            right.output_partitioning(),
            left.schema().fields().len(),
        ),
        JoinType::RightSemi | JoinType::RightAnti => right.output_partitioning().clone(),
        JoinType::Left
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::Full
        | JoinType::LeftMark => {
            Partitioning::UnknownPartitioning(right.output_partitioning().partition_count())
        }
    }
}

/// This function is copied from
/// [`datafusion_physical_plan::physical_plan::execution_plan::boundedness_from_children`].
/// It is used to determine the boundedness of the join operator based on the boundedness of its children.
pub(crate) fn boundedness_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
) -> Boundedness {
    let mut unbounded_with_finite_mem = false;

    for child in children {
        match child.boundedness() {
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            } => {
                return Boundedness::Unbounded {
                    requires_infinite_memory: true,
                }
            }
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            } => {
                unbounded_with_finite_mem = true;
            }
            Boundedness::Bounded => {}
        }
    }

    if unbounded_with_finite_mem {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    } else {
        Boundedness::Bounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn};
    use std::sync::Arc;

    fn create_test_batch(
        schema: Arc<Schema>,
        int_values: Vec<i32>,
        string_values: Vec<String>,
    ) -> RecordBatch {
        let int_array = Arc::new(Int32Array::from(int_values));
        let string_array = Arc::new(StringArray::from(string_values));
        RecordBatch::try_new(schema, vec![int_array, string_array]).unwrap()
    }

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ] as Vec<Field>))
    }

    fn create_probe_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("probe_id", DataType::Int32, false),
            Field::new("probe_name", DataType::Utf8, false),
        ] as Vec<Field>));
        create_test_batch(
            schema,
            vec![100, 200, 300, 400],
            vec![
                "p1".to_string(),
                "p2".to_string(),
                "p3".to_string(),
                "p4".to_string(),
            ],
        )
    }

    fn create_build_batches() -> Vec<RecordBatch> {
        let schema = create_test_schema();
        vec![
            create_test_batch(
                schema.clone(),
                vec![1, 2, 3],
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ),
            create_test_batch(
                schema.clone(),
                vec![4, 5],
                vec!["d".to_string(), "e".to_string()],
            ),
            create_test_batch(
                schema,
                vec![6, 7, 8, 9],
                vec![
                    "f".to_string(),
                    "g".to_string(),
                    "h".to_string(),
                    "i".to_string(),
                ],
            ),
        ]
    }

    #[test]
    fn test_build_batch_from_indices_basic() -> Result<()> {
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();
        let probe_batch = create_probe_batch();

        // Test basic functionality: select some rows from different batches
        let build_positions = vec![(0, 1), (1, 0), (2, 2)]; // batch 0 row 1, batch 1 row 0, batch 2 row 2
        let probe_indices = vec![0, 2, 3]; // probe rows 0, 2, 3

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("probe_id", DataType::Int32, false),
            Field::new("probe_name", DataType::Utf8, false),
        ] as Vec<Field>));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }, // build id
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            }, // build name
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            }, // probe id
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            }, // probe name
        ];

        let result = build_batch_from_indices(
            &schema,
            &build_batch_refs,
            &build_positions,
            &probe_batch,
            &probe_indices,
            &column_indices,
            JoinSide::Left,
        )?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 4);

        // Check build side values
        let build_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let build_names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(build_ids.values(), &[2, 4, 8]); // row 1 from batch 0, row 0 from batch 1, row 2 from batch 2
        assert_eq!(build_names.value(0), "b");
        assert_eq!(build_names.value(1), "d");
        assert_eq!(build_names.value(2), "h");

        // Check probe side values
        let probe_ids = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let probe_names = result
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(probe_ids.values(), &[100, 300, 400]); // probe rows 0, 2, 3
        assert_eq!(probe_names.value(0), "p1");
        assert_eq!(probe_names.value(1), "p3");
        assert_eq!(probe_names.value(2), "p4");

        Ok(())
    }

    #[test]
    fn test_build_batch_from_indices_empty() -> Result<()> {
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();
        let probe_batch = create_probe_batch();

        let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
        let build_positions = vec![(0, 1), (1, 0)];
        let probe_indices = vec![0, 2];
        let column_indices = vec![];

        let result = build_batch_from_indices(
            &schema,
            &build_batch_refs,
            &build_positions,
            &probe_batch,
            &probe_indices,
            &column_indices,
            JoinSide::Left,
        )?;

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 0);

        Ok(())
    }

    #[test]
    fn test_build_batch_from_indices_mark_join() -> Result<()> {
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();
        let probe_batch = create_probe_batch();

        let schema = Arc::new(Schema::new(vec![
            Field::new("probe_id", DataType::Int32, false),
            Field::new("mark", DataType::Boolean, false),
        ] as Vec<Field>));

        let build_positions = vec![(0, 1), (1, 0)];
        let probe_indices = vec![0, 2];

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            }, // probe id
            ColumnIndex {
                index: 0,
                side: JoinSide::None,
            }, // mark column
        ];

        let result = build_batch_from_indices(
            &schema,
            &build_batch_refs,
            &build_positions,
            &probe_batch,
            &probe_indices,
            &column_indices,
            JoinSide::Left,
        )?;

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        // Check probe side values
        let probe_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(probe_ids.values(), &[100, 300]);

        // Check mark column (should be all true for valid indices)
        let marks = result
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(marks.value(0));
        assert!(marks.value(1));

        Ok(())
    }

    #[test]
    fn test_apply_join_filter_to_indices() -> Result<()> {
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();
        let probe_batch = create_probe_batch();

        // Create a simple filter: build.id > probe.id / 100
        let filter_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("probe_id", DataType::Int32, false),
        ] as Vec<Field>));

        let left_expr = Arc::new(PhysicalColumn::new("id", 0));
        let right_expr = Arc::new(PhysicalColumn::new("probe_id", 1));
        let filter_expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }, // build id
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            }, // probe id
        ];

        let filter = JoinFilter::new(filter_expr, column_indices, filter_schema);

        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0), (2, 2)], // ids: 2, 4, 8
            probe: vec![0, 2, 3],                // probe_ids: 100, 300, 400
        };

        let joined_indices = apply_join_filter_to_indices(
            &build_batch_refs,
            &input_indices,
            &probe_batch,
            &filter,
            JoinSide::Left,
        )?;

        // None of the build ids (2, 4, 8) are greater than probe ids (100, 300, 400)
        // So all should be filtered out
        assert!(joined_indices.is_empty());

        Ok(())
    }

    #[test]
    fn test_apply_join_filter_to_indices_with_matches() -> Result<()> {
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();

        // Create a probe batch with smaller values
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("probe_id", DataType::Int32, false),
            Field::new("probe_name", DataType::Utf8, false),
        ] as Vec<Field>));
        let probe_batch = create_test_batch(
            probe_schema,
            vec![1, 3, 5, 7],
            vec![
                "p1".to_string(),
                "p2".to_string(),
                "p3".to_string(),
                "p4".to_string(),
            ],
        );

        // Create a filter: build.id > probe.id
        let filter_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("probe_id", DataType::Int32, false),
        ] as Vec<Field>));

        let left_expr = Arc::new(PhysicalColumn::new("id", 0));
        let right_expr = Arc::new(PhysicalColumn::new("probe_id", 1));
        let filter_expr = Arc::new(BinaryExpr::new(left_expr, Operator::Gt, right_expr));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }, // build id
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            }, // probe id
        ];

        let filter = JoinFilter::new(filter_expr, column_indices, filter_schema);

        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0), (2, 2)], // ids: 2, 4, 8
            probe: vec![0, 2, 3],                // probe_ids: 1, 5, 7
        };

        let joined_indices = apply_join_filter_to_indices(
            &build_batch_refs,
            &input_indices,
            &probe_batch,
            &filter,
            JoinSide::Left,
        )?;

        // Expected matches: (2 > 1), (4 > 5 = false), (8 > 7)
        // So we should get positions 0 and 2
        assert_eq!(joined_indices.build.len(), 2);
        assert_eq!(joined_indices.build, vec![(0, 1), (2, 2)]);
        assert_eq!(joined_indices.probe, vec![0u32, 3u32]);

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_inner() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0), (2, 2)],
            probe: vec![0, 2, 3],
        };
        let adjust_range = 0..5;

        let result =
            adjust_indices_by_join_type(&input_indices, adjust_range, JoinType::Inner, false)?;

        assert_eq!(result.build, input_indices.build);
        assert_eq!(result.probe, input_indices.probe);

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_left() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0)],
            probe: vec![0, 2],
        };
        let adjust_range = 0..5;

        let result =
            adjust_indices_by_join_type(&input_indices, adjust_range, JoinType::Left, false)?;

        assert_eq!(result.build, input_indices.build);
        assert_eq!(result.probe, input_indices.probe);

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_right() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0)],
            probe: vec![0, 2],
        };
        let adjust_range = 0..5;

        let result =
            adjust_indices_by_join_type(&input_indices, adjust_range, JoinType::Right, false)?;

        // Should include unmatched right indices (1, 3, 4)
        assert_eq!(result.build.len(), 5); // original 2 + 3 unmatched

        // First two should be the original matches
        assert_eq!(result.build[0], (0, 1));
        assert_eq!(result.build[1], (1, 0));
        assert_eq!(result.probe[0], 0);
        assert_eq!(result.probe[1], 2);

        // Next three should be null placeholders for unmatched right
        assert_eq!(result.build[2], (-1, -1));
        assert_eq!(result.build[3], (-1, -1));
        assert_eq!(result.build[4], (-1, -1));
        assert_eq!(result.probe[2], 1);
        assert_eq!(result.probe[3], 3);
        assert_eq!(result.probe[4], 4);

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_semi() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0), (2, 2)],
            probe: vec![0, 2, 2], // Note: duplicate right index
        };
        let adjust_range = 0..5;

        let result =
            adjust_indices_by_join_type(&input_indices, adjust_range, JoinType::RightSemi, false)?;

        assert_eq!(result.build, input_indices.build);
        // Should remove duplicates from right side
        assert_eq!(result.probe, vec![0u32, 2u32]); // deduplicated

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_anti() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0)],
            probe: vec![0, 2],
        };
        let adjust_range = 0..5;

        let result =
            adjust_indices_by_join_type(&input_indices, adjust_range, JoinType::RightAnti, false)?;

        assert_eq!(result.build, input_indices.build);
        // Should return indices not in right_indices (1, 3, 4)
        assert_eq!(result.probe, vec![1u32, 3u32, 4u32]);

        Ok(())
    }

    #[test]
    fn test_adjust_indices_by_join_type_left_semi_anti_mark() -> Result<()> {
        let input_indices = JoinedRowsIndices {
            build: vec![(0, 1), (1, 0)],
            probe: vec![0, 2],
        };
        let adjust_range = 0..5;

        for join_type in [JoinType::LeftSemi, JoinType::LeftAnti, JoinType::LeftMark] {
            let result = adjust_indices_by_join_type(
                &input_indices,
                adjust_range.clone(),
                join_type,
                false,
            )?;

            // These join types should return empty results
            assert!(result.is_empty());
        }

        Ok(())
    }

    #[test]
    fn test_get_anti_indices() {
        let range = 0..5;
        let input_indices = vec![0u32, 2u32, 4u32];
        let result = get_anti_indices(range, &input_indices);
        assert_eq!(result, vec![1u32, 3u32]);
    }

    #[test]
    fn test_get_anti_indices_empty_input() {
        let range = 0..3;
        let input_indices: Vec<u32> = vec![];
        let result = get_anti_indices(range, &input_indices);
        assert_eq!(result, vec![0u32, 1u32, 2u32]);
    }

    #[test]
    fn test_get_anti_indices_full_coverage() {
        let range = 0..3;
        let input_indices = vec![0u32, 1u32, 2u32];
        let result = get_anti_indices(range, &input_indices);
        assert_eq!(result, Vec::<u32>::new());
    }

    #[test]
    fn test_get_semi_indices() {
        let range = 0..5;
        let input_indices = vec![0u32, 2u32, 4u32, 6u32]; // 6 is outside range
        let result = get_semi_indices(range, &input_indices);
        assert_eq!(result, vec![0u32, 2u32, 4u32]);
    }

    #[test]
    fn test_get_semi_indices_with_duplicates() {
        let range = 0..5;
        let input_indices = vec![0u32, 2u32, 2u32, 4u32]; // duplicate 2
        let result = get_semi_indices(range, &input_indices);
        assert_eq!(result, vec![0u32, 2u32, 4u32]); // should be deduplicated
    }

    #[test]
    fn test_get_semi_indices_empty_input() {
        let range = 0..3;
        let input_indices: Vec<u32> = vec![];
        let result = get_semi_indices(range, &input_indices);
        assert_eq!(result, Vec::<u32>::new());
    }

    #[test]
    fn test_append_probe_indices_in_order() {
        let left_indices = vec![(0, 1), (1, 0)];
        let right_indices = vec![1u32, 3u32];
        let range = 0..5;

        let (result_left, result_right) =
            append_probe_indices_in_order(&left_indices, &right_indices, range);

        // Should fill in missing indices with null placeholders
        // Expected: [0, 1, 2, 3, 4] for right
        // With left: [null, (0,1), null, (1,0), null]
        assert_eq!(result_right, vec![0u32, 1u32, 2u32, 3u32, 4u32]);
        assert_eq!(
            result_left,
            vec![(-1, -1), (0, 1), (-1, -1), (1, 0), (-1, -1)]
        );
    }

    #[test]
    fn test_multi_batch_interleave_optimization() -> Result<()> {
        // Test that we only process batches we actually need
        let build_batches = create_build_batches();
        let build_batch_refs: Vec<&RecordBatch> = build_batches.iter().collect();
        let probe_batch = create_probe_batch();

        // Only select from batch 0 and batch 2, skip batch 1
        let build_positions = vec![(0, 1), (2, 2)];
        let probe_indices = vec![0, 2];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("probe_id", DataType::Int32, false),
        ] as Vec<Field>));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }, // build id
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            }, // probe id
        ];

        let result = build_batch_from_indices(
            &schema,
            &build_batch_refs,
            &build_positions,
            &probe_batch,
            &probe_indices,
            &column_indices,
            JoinSide::Left,
        )?;

        assert_eq!(result.num_rows(), 2);

        let build_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(build_ids.values(), &[2, 8]); // row 1 from batch 0, row 2 from batch 2

        let probe_ids = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(probe_ids.values(), &[100, 300]);

        Ok(())
    }
}
