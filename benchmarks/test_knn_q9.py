#!/usr/bin/env python3
"""
Test KNN execution timing - measure the actual query execution with .count()
"""
import os
import sys
import time

# Use the installed sedonadb package (via maturin develop)
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sedonadb as sd

def test_knn_execution_timing():

    data_path = "/Users/feng/temp/SpatialBench_sf=1_format=parquet"
    
    if not os.path.exists(data_path):
        print(f"❌ Data not found at {data_path}")
        return 1
        
    building_path = f"{data_path}/building/*.parquet"
    trip_path = f"{data_path}/trip/*.parquet"
    
    print("🧪 Testing KNN Execution Timing (including .count())")
    print()
    
    # Setup SedonaDB context
    ctx = sd.connect()
    
    # Load limited data for testing
    building_df = ctx.read_parquet(building_path)
    building_df.to_view('buildings', overwrite=True)

    # Load all trips first, then use SQL to get specific trip (avoiding .limit() bug)
    all_trips_df = ctx.read_parquet(trip_path)
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