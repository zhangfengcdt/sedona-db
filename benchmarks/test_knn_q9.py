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
"""
Test KNN execution timing - measure the actual query execution with .count()
"""
import sys
import time

import sedonadb as sd

### TODO: this is a temp file (test) and will be removed later before merging to main repo

def test_knn_execution_timing():

    data_path = "s3://wherobots-benchmark-prod/SpatialBench_sf=1_format=parquet"
    
    building_path = f"{data_path}/building/"
    trip_path = f"{data_path}/trip/"
    
    print("🧪 Testing KNN Execution Timing (including .count())")
    print()
    
    # Setup SedonaDB context
    ctx = sd.connect()
    
    # Load limited data for testing
    building_df = ctx.read_parquet(building_path, options={"aws.skip_signature": True, "aws.region": "us-west-2"})
    building_df.to_view('buildings', overwrite=True)

    # Load all trips first, then use SQL to get specific trip (avoiding .limit() bug)
    all_trips_df = ctx.read_parquet(trip_path, options={"aws.skip_signature": True, "aws.region": "us-west-2"})
    all_trips_df.to_view('all_trips', overwrite=True)
    
    # Get specific trip using SQL to avoid .limit() issues
    # trip_df = ctx.sql("SELECT * FROM all_trips WHERE t_tripkey = 1")
    trip_df = ctx.sql("SELECT * FROM all_trips")
    trip_df.to_view('trips', overwrite=True)
    # print(trip_df.to_pandas())
    
    print(f"✅ Loaded {building_df.count():,} buildings and {trip_df.count():,} trips")
    print()
    
    # Test the exact Q9 query structure
    q9_query = """
        SELECT 
            t.t_tripkey,
            b.b_buildingkey,
            b.b_name,
            ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) as distance
        FROM trips t, buildings b
        WHERE ST_KNN(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 5, FALSE)
    """
    
    print("🔄 Running Q9 query and measuring execution time...")

    # Measure the actual execution (what you suggested)
    start_time = time.time()
    result = ctx.sql(q9_query)
    result_count = result.count()
    execution_time = time.time() - start_time

    # Convert the result to a Pandas DataFrame and print it
    # result_df = result.to_pandas()
    # print(result_df)

    print(f"⏱️  Total execution time: {execution_time:.3f}s for {result_count:,} results")

    return 0

if __name__ == "__main__":
    sys.exit(test_knn_execution_timing())