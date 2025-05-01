import json
import urllib.request
import shutil

from pathlib import Path

HERE = Path(__file__).parent

with open(HERE / "geoarrow-data" / "manifest.json") as f:
    manifest = json.load(f)

for group in manifest["groups"]:
    group_name = group["name"]
    for file in group["files"]:
        url = file["url"]
        filename = Path(url).name
        local_path = HERE / "geoarrow-data" / group_name / "files" / filename
        if local_path.exists():
            print(f"Using cached '{filename}'")
        elif file["format"] in ("parquet", "geoparquet"):
            # Only download Parquet/GeoParquet versions of asset files to save space
            print(f"Downloading {url}")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(url) as fin, open(local_path, "wb") as fout:
                shutil.copyfileobj(fin, fout)

print("Done!")
