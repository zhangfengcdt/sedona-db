# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import pytest
from test_bench_base import TestBenchBase
from sedonadb.testing import SedonaDB


class TestBenchKNN(TestBenchBase):
    def setup_class(self):
        """Setup test data for KNN benchmarks"""
        self.sedonadb = SedonaDB.create_or_skip()

        # Create building-like polygons (index side - fewer, larger geometries)
        # Note: Dataset sizes are limited to avoid performance issues observed when processing
        # very large synthetic datasets. Large synthetic datasets have been observed to cause
        # memory pressure or performance degradation in DataFusion operations.
        building_options = {
            "geom_type": "Polygon",
            "target_rows": 2_000,  # Reasonable size for benchmarking
            "vertices_per_linestring_range": [4, 8],
            "size_range": [0.001, 0.01],
            "seed": 42,
        }

        building_query = f"""
            SELECT
                geometry as geom,
                round(random() * 1000) as building_id,
                'Building_' || cast(round(random() * 1000) as varchar) as name
            FROM sd_random_geometry('{json.dumps(building_options)}')
        """
        building_tab = self.sedonadb.execute_and_collect(building_query)
        self.sedonadb.create_table_arrow("knn_buildings", building_tab)

        # Create trip pickup points (probe side - many small geometries)
        trip_options = {
            "geom_type": "Point",
            "target_rows": 10_000,
            "seed": 43,
        }

        trip_query = f"""
            SELECT
                geometry as geom,
                round(random() * 100000) as trip_id
            FROM sd_random_geometry('{json.dumps(trip_options)}')
        """
        trip_tab = self.sedonadb.execute_and_collect(trip_query)
        self.sedonadb.create_table_arrow("knn_trips", trip_tab)

        # Create a smaller test dataset for quick tests
        small_building_query = """
            SELECT * FROM knn_buildings LIMIT 1000
        """
        small_building_tab = self.sedonadb.execute_and_collect(small_building_query)
        self.sedonadb.create_table_arrow("knn_buildings_small", small_building_tab)

        small_trip_query = """
            SELECT * FROM knn_trips LIMIT 5000
        """
        small_trip_tab = self.sedonadb.execute_and_collect(small_trip_query)
        self.sedonadb.create_table_arrow("knn_trips_small", small_trip_tab)

    @pytest.mark.parametrize("k", [1, 5, 10])
    @pytest.mark.parametrize("use_spheroid", [False, True])
    @pytest.mark.parametrize("dataset_size", ["small", "large"])
    def test_knn_performance(self, benchmark, k, use_spheroid, dataset_size):
        """Benchmark KNN query performance with different parameters"""

        if dataset_size == "small":
            trip_table = "knn_trips_small"
            building_table = "knn_buildings_small"
            trip_limit = 100  # Test with 100 trips
        else:
            trip_table = "knn_trips_small"
            building_table = "knn_buildings"
            trip_limit = 500

        spheroid_str = "TRUE" if use_spheroid else "FALSE"

        def run_knn_query():
            query = f"""
                WITH trip_sample AS (
                    SELECT trip_id, geom as trip_geom
                    FROM {trip_table}
                    LIMIT {trip_limit}
                ),
                building_with_geom AS (
                    SELECT building_id, name, geom as building_geom
                    FROM {building_table}
                )
                SELECT
                    t.trip_id,
                    b.building_id,
                    b.name,
                    ST_Distance(t.trip_geom, b.building_geom) as distance
                FROM trip_sample t
                JOIN building_with_geom b ON ST_KNN(t.trip_geom, b.building_geom, {k}, {spheroid_str})
                ORDER BY t.trip_id, distance
            """
            result = self.sedonadb.execute_and_collect(query)
            return len(result)  # Return result count for verification

        # Run the benchmark
        result_count = benchmark(run_knn_query)

        # Verify we got the expected number of results (trips * k)
        expected_count = trip_limit * k
        assert result_count == expected_count, (
            f"Expected {expected_count} results, got {result_count}"
        )

    @pytest.mark.parametrize("k", [1, 5, 10, 20])
    def test_knn_scalability_by_k(self, benchmark, k):
        """Test how KNN performance scales with increasing k values"""

        def run_knn_query():
            query = f"""
                WITH trip_sample AS (
                    SELECT trip_id, geom as trip_geom
                    FROM knn_trips_small
                    LIMIT 50  -- Small sample for k scaling test
                )
                SELECT
                    COUNT(*) as result_count
                FROM trip_sample t
                JOIN knn_buildings_small b ON ST_KNN(t.trip_geom, b.geom, {k}, FALSE)
            """
            result = self.sedonadb.execute_and_collect(query)
            return result.to_pandas().iloc[0]["result_count"]

        result_count = benchmark(run_knn_query)
        expected_count = 50 * k  # 50 trips * k neighbors each
        assert result_count == expected_count, (
            f"Expected {expected_count} results, got {result_count}"
        )

    def test_knn_correctness(self):
        """Verify KNN returns results in correct distance order"""

        # Test with a known point and verify ordering
        query = """
            WITH test_point AS (
                SELECT ST_Point(0.0, 0.0) as query_geom
            )
            SELECT
                ST_Distance(test_point.query_geom, b.geom) as distance,
                b.building_id
            FROM test_point
            JOIN knn_buildings_small b ON ST_KNN(test_point.query_geom, b.geom, 5, FALSE)
            ORDER BY distance
        """

        result = self.sedonadb.execute_and_collect(query).to_pandas()

        # Verify we got 5 results
        assert len(result) == 5, f"Expected 5 results, got {len(result)}"

        # Verify distances are in ascending order
        distances = result["distance"].tolist()
        assert distances == sorted(distances), (
            f"Results not ordered by distance: {distances}"
        )

        # Verify all distances are non-negative
        assert all(d >= 0 for d in distances), f"Found negative distances: {distances}"

    def test_knn_tie_breaking(self):
        """Test KNN behavior with tie-breaking when geometries have equal distances"""

        # Create test data with known equal distances
        setup_query = """
            WITH test_points AS (
                SELECT 1 as id, ST_Point(1.0, 0.0) as geom
                UNION ALL
                SELECT 2 as id, ST_Point(-1.0, 0.0) as geom
                UNION ALL
                SELECT 3 as id, ST_Point(0.0, 1.0) as geom
                UNION ALL
                SELECT 4 as id, ST_Point(0.0, -1.0) as geom
                UNION ALL
                SELECT 5 as id, ST_Point(2.0, 0.0) as geom
            )
            SELECT * FROM test_points
        """
        tie_test_tab = self.sedonadb.execute_and_collect(setup_query)
        self.sedonadb.create_table_arrow("knn_tie_test", tie_test_tab)

        # Query for 2 nearest neighbors from origin - should get 2 of the 4 equidistant points
        query = """
            WITH query_point AS (
                SELECT ST_Point(0.0, 0.0) as geom
            )
            SELECT
                t.id,
                ST_Distance(query_point.geom, t.geom) as distance
            FROM query_point
            JOIN knn_tie_test t ON ST_KNN(query_point.geom, t.geom, 2, FALSE)
            ORDER BY distance, t.id
        """

        result = self.sedonadb.execute_and_collect(query).to_pandas()

        # Should get exactly 2 results
        assert len(result) == 2, f"Expected 2 results, got {len(result)}"

        # Both should be at distance 1.0 (the 4 equidistant points)
        distances = result["distance"].tolist()
        assert all(abs(d - 1.0) < 1e-6 for d in distances), (
            f"Expected distances ~1.0, got {distances}"
        )
