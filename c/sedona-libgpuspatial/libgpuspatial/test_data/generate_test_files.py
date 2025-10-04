from pathlib import Path

import pyarrow as pa
import geoarrow.pyarrow as ga
import numpy as np
import random as rand  # For random number generation

from spider_generator import UniformGenerator, WKTSinkInMemory, PointToPolygonSink

HERE = Path(__file__).parent


def generate_basic_test_points(n_row_groups=100, n_per_row_group=1000, seed=12345):
    rng = np.random.default_rng(seed)

    schema = pa.schema({"idx": pa.int64(), "geometry": ga.wkb()})
    with pa.ipc.new_stream(HERE / "test_points.arrows", schema) as writer:
        for _ in range(n_row_groups):
            x = rng.uniform(low=-100, high=100, size=n_per_row_group)
            y = rng.uniform(low=-100, high=100, size=n_per_row_group)
            table = pa.table(
                {
                    "idx": range(n_per_row_group),
                    "geometry": ga.as_wkb(ga.point().from_geobuffers(None, x, y)),
                }
            )
            writer.write_table(table, max_chunksize=n_per_row_group)


def generate_basic_test_polygons(filename="", n_row_groups=100, n_per_row_group=1000, seed=12345):
    dimensions = 2
    maxseg = 8
    polysize = 0.5
    rand.seed(seed)

    schema = pa.schema({"idx": pa.int64(), "geometry": ga.wkb()})
    with pa.ipc.new_stream(HERE / filename, schema) as writer:
        for _ in range(n_row_groups):
            output = []
            datasink = WKTSinkInMemory(output)
            datasink = PointToPolygonSink(datasink, maxseg, polysize)
            generator = UniformGenerator(n_per_row_group, dimensions)
            generator.setSink(datasink)
            generator.generate()
            polygons = ga.as_geoarrow(output)
            table = pa.table(
                {
                    "idx": range(n_per_row_group),
                    "geometry": ga.as_wkb(polygons),
                }
            )
            writer.write_table(table, max_chunksize=n_per_row_group)


if __name__ == "__main__":
    generate_basic_test_points()
    generate_basic_test_polygons("test_polygons.arrows")
    generate_basic_test_polygons("test_polygons1.arrows", n_row_groups=100, n_per_row_group=500, seed=234)
    generate_basic_test_polygons("test_polygons2.arrows", n_row_groups=100, n_per_row_group=1000, seed=567)
