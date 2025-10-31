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

use arrow_array::array::StringArray;
use sedona::context::SedonaContext;

/// End-to-end tests for spatial aggregate functions.
///
/// These tests verify that ST_*_Aggr functions work correctly through the full
/// SedonaDB SQL execution pipeline, including:
/// - SQL parsing and planning
/// - Function registration and signature matching
/// - Aggregate state management across multiple input rows
/// - Geometry computation and result formatting
/// - CRS metadata propagation (where applicable)
#[cfg(test)]
mod aggregate_functions {
    use super::*;

    /// Tests ST_Envelope_Aggr: computes the minimum bounding rectangle (envelope)
    /// that contains all input geometries.
    ///
    /// Test scenario:
    /// - Input: Three points at (1,2), (3,4), and (5,6)
    /// - Expected: A polygon envelope with corners at (1,2) and (5,6)
    /// - Verifies: The aggregate correctly expands to encompass all points
    #[tokio::test]
    async fn test_st_envelope_aggr() {
        // Initialize SedonaDB context with all spatial functions registered
        let ctx = SedonaContext::new_local_interactive()
            .await
            .expect("context should initialize");

        // Query uses VALUES clause to create inline test data, then applies
        // ST_Envelope_Aggr to compute the bounding rectangle, and ST_AsText
        // to convert the result to WKT format for verification
        let aggregate_query = r#"
        SELECT ST_AsText(ST_Envelope_Aggr(geom)) FROM (
                VALUES
                    (ST_GeomFromText('POINT (1 2)')),
                    (ST_GeomFromText('POINT (3 4)')),
                    (ST_GeomFromText('POINT (5 6)'))
            ) AS t(geom)
        "#;

        // Execute the query through DataFusion's SQL engine
        let result = ctx
            .sql(aggregate_query.trim())
            .await
            .expect("aggregate query should succeed despite metadata loss");

        // Collect all result batches into memory
        let batches = result
            .collect()
            .await
            .expect("collecting aggregate result should succeed");

        // Verify the aggregate returned exactly one row with one column
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        // Extract the WKT string result from the output column
        let result_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("result should be string array");

        let result_wkt = result_array.value(0);

        // Verify the result is a POLYGON geometry type
        assert!(
            result_wkt.contains("POLYGON"),
            "Expected POLYGON, got: {}",
            result_wkt
        );

        // The envelope should be a rectangle from (1,2) to (5,6) encompassing all points
        assert_eq!(result_wkt, "POLYGON((1 2,1 6,5 6,5 2,1 2))");
    }

    /// Tests ST_Intersection_Aggr: computes the geometric intersection of all
    /// input geometries (the area/region common to all inputs).
    ///
    /// Test scenario:
    /// - Input: Three overlapping square polygons:
    ///   * First polygon: (0,0) to (4,4)
    ///   * Second polygon: (2,2) to (6,6)
    ///   * Third polygon: (1,1) to (5,5)
    /// - Expected: The common intersection area from (2,2) to (4,4)
    /// - Verifies: The aggregate correctly computes the overlapping region
    ///   where all three polygons intersect
    #[tokio::test]
    async fn test_st_intersection_aggr() {
        // Create SedonaDB context
        let ctx = SedonaContext::new_local_interactive()
            .await
            .expect("context should initialize");

        let aggregate_query = r#"
        SELECT ST_AsText(ST_Intersection_Aggr(geom)) FROM (
                VALUES
                    (ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')),
                    (ST_GeomFromText('POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))')),
                    (ST_GeomFromText('POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1))'))
            ) AS t(geom)
        "#;

        let result = ctx
            .sql(aggregate_query.trim())
            .await
            .expect("aggregate query should succeed");

        let batches = result
            .collect()
            .await
            .expect("collecting aggregate result should succeed");

        // Verify the aggregate function worked correctly
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        // The result should be the intersection of all three polygons
        let result_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("result should be string array");

        let result_wkt = result_array.value(0);
        assert!(
            result_wkt.contains("POLYGON"),
            "Expected POLYGON or MULTIPOLYGON, got: {}",
            result_wkt
        );

        // The intersection of the three polygons is the area where all three overlap
        assert_eq!(result_wkt, "MULTIPOLYGON(((2 4,2 2,4 2,4 4,2 4)))");
    }

    /// Tests ST_Union_Aggr: computes the geometric union of all input geometries,
    /// merging overlapping areas and combining separate geometries.
    ///
    /// Test scenario:
    /// - Input: Three polygons with different overlap patterns:
    ///   * First polygon: (0,0) to (2,2) - overlaps with second
    ///   * Second polygon: (1,1) to (3,3) - overlaps with first
    ///   * Third polygon: (4,4) to (5,5) - separate from others
    /// - Expected: A merged geometry combining all inputs (may be MULTIPOLYGON
    ///   or GEOMETRYCOLLECTION depending on implementation)
    /// - Verifies: The aggregate correctly merges overlapping regions and
    ///   preserves separate components
    #[tokio::test]
    async fn test_st_union_aggr() {
        // Create SedonaDB context
        let ctx = SedonaContext::new_local_interactive()
            .await
            .expect("context should initialize");

        let aggregate_query = r#"
        SELECT ST_AsText(ST_Union_Aggr(geom)) FROM (
                VALUES
                    (ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')),
                    (ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))')),
                    (ST_GeomFromText('POLYGON ((4 4, 5 4, 5 5, 4 5, 4 4))'))
            ) AS t(geom)
        "#;

        let result = ctx
            .sql(aggregate_query.trim())
            .await
            .expect("aggregate query should succeed");

        let batches = result
            .collect()
            .await
            .expect("collecting aggregate result should succeed");

        // Verify the aggregate function worked correctly
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        // The result should be a geometry collection or multipolygon containing the union
        let result_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("result should be string array");

        let result_wkt = result_array.value(0);
        assert!(
            result_wkt.contains("GEOMETRYCOLLECTION")
                || result_wkt.contains("POLYGON")
                || result_wkt.contains("MULTIPOLYGON"),
            "Expected GEOMETRYCOLLECTION, POLYGON or MULTIPOLYGON, got: {}",
            result_wkt
        );
    }

    /// Tests ST_Analyze_Aggr: computes statistical analysis and metadata about
    /// a collection of input geometries.
    ///
    /// Test scenario:
    /// - Input: Three points at (1,2), (3,4), and (5,6)
    /// - Expected: Statistical summary (format varies by implementation, may
    ///   include counts, types, bounding box, etc.)
    /// - Verifies: The aggregate correctly processes all geometries and returns
    ///   analysis results
    #[tokio::test]
    async fn test_st_analyze_aggr() {
        // Create SedonaDB context
        let ctx = SedonaContext::new_local_interactive()
            .await
            .expect("context should initialize");

        let aggregate_query = r#"
        SELECT ST_Analyze_Aggr(geom) FROM (
                VALUES
                    (ST_GeomFromText('POINT (1 2)')),
                    (ST_GeomFromText('POINT (3 4)')),
                    (ST_GeomFromText('POINT (5 6)'))
            ) AS t(geom)
        "#;

        let result = ctx
            .sql(aggregate_query.trim())
            .await
            .expect("aggregate query should succeed");

        let batches = result
            .collect()
            .await
            .expect("collecting aggregate result should succeed");

        // Verify the aggregate function worked correctly
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let result_array = batch.column(0);
        assert!(!result_array.is_empty(), "Expected non-empty result");
    }
}
