# SedonaDB

SedonaDB is a high-performance, dependency-free geospatial compute engine.

You can easily run SedonaDB locally or in the cloud.  The first release supports a core set of vector operations, but the full-suite of common vector and raster computations will be supported soon.

SedonaDB only runs on a single machine, so it’s perfect for processing smaller datasets.  You can use SedonaSpark, SedonaFlink, or SedonaSnow for operations on larger datasets.

## Install

You can install Python SedonaDB with `pip install apache-sedona`.

You can also install Rust SedonaDB with `cargo add apache-sedona`.

## Overture buildings example

This section shows how to query the Overture buildings set which is stored in GeoParquet.

Start by downloading the Overture buildings dataset for a specific geographic region:

```
pip install overturemaps
overturemaps download --type=building --bbox=-79.9390,32.7725,-79.9212,32.7813 -f geoparquet -o buildings_sample.parquet
```

Start by establishing a connection:

```python
import sedona.db as sedonadb
sedona = sedonadb.connect()
```

Read the dataset into a Python SedonaDB table:

```python
path = "buildings_sample.parquet"
df = sedona.read_parquet(path)
```

Now run a query to compute the area of tall buildings (above 20 meters):

```python
df.to_view("buildings")
sedona.sql("""
SELECT
    id,
    height,
    num_floors,
    roof_shape,
    ST_Area(geometry) as area
FROM
    buildings
WHERE
    is_underground = FALSE
    AND height IS NOT NULL
    AND height > 20
ORDER BY
    height DESC
LIMIT 50;
""").show()
```

Here's the query output:

```
┌──────────────────────────────────────┬─────────┬────────────┬────────────┬───────────────────────┐
│                  id                  ┆  height ┆ num_floors ┆ roof_shape ┆          area         │
│               utf8view               ┆ float64 ┆    int32   ┆  utf8view  ┆        float64        │
╞══════════════════════════════════════╪═════════╪════════════╪════════════╪═══════════════════════╡
│ 76235898-166c-435b-9760-26f4940900cb ┆    37.0 ┆          8 ┆            ┆  7.549477499915754e-8 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 33d9cfae-aea7-417c-945b-00a7d84ab28e ┆    24.0 ┆          3 ┆ gabled     ┆  8.683507500163057e-8 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 5a5f8d21-be6c-480d-934a-e2d73e52656b ┆    24.0 ┆          3 ┆ hipped     ┆ 5.6109364998276947e-8 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 96c8306a-2eab-42cc-b53f-2339ee457610 ┆    24.0 ┆          5 ┆ flat       ┆  4.708836600040366e-7 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 86956a65-f781-4810-83d3-c0e452df69bc ┆    23.0 ┆          4 ┆            ┆ 1.1721439000173588e-7 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 4f79516c-c892-44b0-b6d4-d3e78df2ec82 ┆    23.0 ┆          3 ┆ hipped     ┆  1.627074450006598e-7 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 236a2bd3-a889-4afb-8fd7-2c15cf27af09 ┆    21.0 ┆            ┆ pyramidal  ┆  1.744069999920037e-9 │
└──────────────────────────────────────┴─────────┴────────────┴────────────┴───────────────────────┘
```

## Features of SedonaDB

SedonaDB has several advantages:

* The code is written in Rust and runs fast.
* It supports both vector and raster functions.  You can use a single library to access a full-suite of spatial functionality.
* It always propagates the coordinate reference system (CRS).
* It supports legacy and modern file formats.
* It has Python and SQL APIs and users can seamlessly switch between them.
* It’s easily extensible and customized.
* It is interoperable with other PyArrow compatible libraries like GeoPandas, DuckDB, and Polars.
* It has a great community of maintainers and encourages external contributions.

## Contributing

There are many different ways to contribute to SedonaDB:

* Join the Discord and chat with us
* Open a GitHub Discussion with questions or ideas
* Work on an existing issue.  Just comment “take” on the issue and we will assign you the task.
* Brainstorm features with the contributors and then contribute a pull request.

The contributors meet on a monthly basis and we’re happy to add you to the call if you would like to join the community!

## Community

SedonaDB is a subproject of Apache Sedona, an Apache Software Foundation project.

The project is governed by the Apache Software Foundation and subject to all the rules and oversight requirements.
