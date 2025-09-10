#!/usr/bin/env python3
"""
Test KNN execution timing - measure the actual query execution with .count()
"""
import os
import sys
import time

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sedonadb as sd

def test_knn_execution_timing():

    trips_processed = 1

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
    
    trip_df = ctx.read_parquet(trip_path).limit(trips_processed)
    trip_df.to_view('trips', overwrite=True)
    
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
    
    per_trip_ms = (execution_time / trips_processed)
    trips_per_sec = trips_processed / execution_time
    expected_results = trips_processed * 5
    
    print(f"⏱️  Total execution time: {execution_time:.3f}s")
    print(f"📊 Results returned: {result_count:,} (expected: {expected_results:,})")
    print(f"⚡ Per trip: {per_trip_ms:.2f}ms")
    print(f"🔥 Throughput: {trips_per_sec:.1f} trips/second")
    print()
    
    # Extrapolate to full dataset
    full_trip_count = 6_000_000  # Full SpatialBench dataset
    projected_time_seconds = (per_trip_ms / 1000) * full_trip_count
    projected_hours = projected_time_seconds / 3600
    projected_minutes = projected_time_seconds / 60
    
    print("🔮 Full Q9 Performance Projection:")
    print(f"📊 Dataset: {full_trip_count:,} trips × k=5 = {full_trip_count * 5:,} KNN results")
    
    if projected_hours < 1:
        time_str = f"{projected_minutes:.1f} minutes"
        assessment = "🚀 EXCELLENT" if projected_minutes < 60 else "✅ GOOD"
    else:
        time_str = f"{projected_hours:.1f} hours"
        assessment = "⚠️ SLOW" if projected_hours > 2 else "✅ ACCEPTABLE"
    
    print(f"⏱️  Estimated time: {time_str}")
    print(f"🎯 Assessment: {assessment}")
    
    print()
    print("🎯 Key Findings:")
    if per_trip_ms < 1:
        print("   🚀 Excellent KNN performance - optimization is working perfectly!")
    elif per_trip_ms < 10:
        print("   ✅ Good KNN performance - clear optimization benefits")
    elif per_trip_ms < 50:
        print("   ⚠️  Moderate performance - there may be additional bottlenecks")
    else:
        print("   ❌ Slow performance - optimization may not be fully effective")
    
    return 0

if __name__ == "__main__":
    sys.exit(test_knn_execution_timing())