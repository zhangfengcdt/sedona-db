from pathlib import Path
import geopandas as gpd
import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd # pandas is implicitly used by GeoPandas
from shapely.geometry import shape # Used internally by GeoPandas

# Define the number of rows to extract for each type
N_SAMPLES = 100

file_path = "ns-water_land-poly_wkb.arrows"

try:
    with ipc.open_stream(file_path) as reader:
        table = reader.read_all()
        # Convert to pandas DataFrame and sample 1000 rows for the working set
        # df_working = table.to_pandas().sample(n=1000, random_state=1)
        df_working = table.to_pandas()
        # Convert the WKB column to a GeoSeries and assign it back to the DataFrame
        # This converts the DataFrame to a GeoDataFrame
        geometry_series = gpd.GeoSeries.from_wkb(df_working["geometry"])
        gdf_working = gpd.GeoDataFrame(df_working, geometry=geometry_series, crs=None)

        # --- 1. Extract 100 Polygons ---

        # Filter rows where the geometry type is 'Polygon'
        polygon_mask = gdf_working.geometry.geom_type == "Polygon"
        polygons = gdf_working[polygon_mask]

        # Take the first N_SAMPLES (100) Polygons found
        polygons_100 = polygons.head(N_SAMPLES)

        # --- 2. Extract 100 MultiPolygons ---

        # Filter rows where the geometry type is 'MultiPolygon'
        multipolygon_mask = gdf_working.geometry.geom_type == "MultiPolygon"
        multipolygons = gdf_working[multipolygon_mask]

        # Take the first N_SAMPLES (100) MultiPolygons found
        multipolygons_100 = multipolygons.head(N_SAMPLES)

        # --- Verification ---
        print("Extraction Complete.")
        print(f"Polygons extracted: {len(polygons_100)} (Target: {N_SAMPLES})")
        print(f"MultiPolygons extracted: {len(multipolygons_100)} (Target: {N_SAMPLES})")

        if len(polygons_100) < N_SAMPLES or len(multipolygons_100) < N_SAMPLES:
            print("\nNote: The initial 1000-row sample did not contain enough geometries of one or both types.")

        # --- Result ---
        # The two resulting GeoDataFrames are: polygons_100 and multipolygons_100.
        # You can combine them if needed:
        combined_gdf = pd.concat([polygons_100, multipolygons_100])
        print(f"Combined GeoDataFrame size: {len(combined_gdf)}")

except FileNotFoundError:
    print(f"Error: The file {file_path} was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")