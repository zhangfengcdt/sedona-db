import geoarrow.pyarrow as ga
import geopandas.testing
import pandas as pd
import pyarrow as pa
import pytest
import sedona_rs


def test_dataframe_to_arrow(con):
    df = con.sql("SELECT 1 as one, ST_GeomFromWKT('POINT (0 1)') as geom")
    expected_schema = pa.schema(
        [pa.field("one", pa.int64(), nullable=False), pa.field("geom", ga.wkb())]
    )

    assert pa.schema(df) == expected_schema
    assert df.to_arrow_table() == pa.table(
        {"one": [1], "geom": ga.as_wkb(["POINT (0 1)"])}, schema=expected_schema
    )

    # Make sure we can request a schema if the schema is identical
    assert df.to_arrow_table(schema=expected_schema) == df.to_arrow_table()

    # ...but not otherwise (yet)
    with pytest.raises(
        sedona_rs._lib.SedonaError,
        match="Requested schema != DataFrame schema not yet supported",
    ):
        df.to_arrow_table(schema=pa.schema({}))


def test_dataframe_to_pandas(con):
    # Check with a geometry column
    df_with_geo = con.sql("SELECT 1 as one, ST_GeomFromWKT('POINT (0 1)') as geom")
    geopandas.testing.assert_geodataframe_equal(
        df_with_geo.to_pandas(),
        geopandas.GeoDataFrame(
            {"one": [1], "geom": geopandas.GeoSeries.from_wkt(["POINT (0 1)"])}
        ).set_geometry("geom"),
    )

    # Check with more than one geometry column
    df_with_multi_geo = con.sql(
        "SELECT ST_GeomFromWKT('POINT (0 1)') as geom1, ST_GeomFromWKT('POINT (2 3)') as geom2"
    )
    geodf_with_multi_geo = geopandas.GeoDataFrame(
        {
            "geom1": geopandas.GeoSeries.from_wkt(["POINT (0 1)"]),
            "geom2": geopandas.GeoSeries.from_wkt(["POINT (2 3)"]),
        }
    )

    geopandas.testing.assert_geodataframe_equal(
        df_with_multi_geo.to_pandas(geometry="geom1"),
        geodf_with_multi_geo.set_geometry("geom1"),
    )

    geopandas.testing.assert_geodataframe_equal(
        df_with_multi_geo.to_pandas(geometry="geom2"),
        geodf_with_multi_geo.set_geometry("geom2"),
    )

    # Check without geometry column
    df_without_geo = con.sql("SELECT 1 as one")
    pd.testing.assert_frame_equal(
        df_without_geo.to_pandas(), pd.DataFrame({"one": [1]})
    )
