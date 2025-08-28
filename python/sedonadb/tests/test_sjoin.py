import pytest
import json
from sedonadb.testing import PostGIS, SedonaDB


@pytest.mark.parametrize(
    "join_type", ["INNER JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN"]
)
@pytest.mark.parametrize(
    "on",
    [
        "ST_Intersects(sjoin_point.geometry, sjoin_polygon.geometry)",
        "ST_Within(sjoin_point.geometry, sjoin_polygon.geometry)",
        "ST_Contains(sjoin_polygon.geometry, sjoin_point.geometry)",
        "ST_DWithin(sjoin_point.geometry, sjoin_polygon.geometry, 1.0)",
        "ST_DWithin(sjoin_point.geometry, sjoin_polygon.geometry, sjoin_point.dist / 100)",
        "ST_DWithin(sjoin_point.geometry, sjoin_polygon.geometry, sjoin_polygon.dist / 100)",
    ],
)
def test_spatial_join(join_type, on):
    eng_sedonadb = SedonaDB.create_or_skip()
    eng_postgis = PostGIS.create_or_skip()

    options = json.dumps(
        {
            "geom_type": "Point",
            "polygon_hole_rate": 0.5,
            "num_parts_range": [2, 10],
            "vertices_per_linestring_range": [2, 10],
            "seed": 42,
        }
    )
    df_point = eng_sedonadb.execute_and_collect(
        f"SELECT * FROM sd_random_geometry('{options}') LIMIT 100"
    )
    options = json.dumps(
        {
            "geom_type": "Polygon",
            "polygon_hole_rate": 0.5,
            "num_parts_range": [2, 10],
            "vertices_per_linestring_range": [2, 10],
            "seed": 43,
        }
    )
    df_polygon = eng_sedonadb.execute_and_collect(
        f"SELECT * FROM sd_random_geometry('{options}') LIMIT 100"
    )
    eng_sedonadb.create_table_arrow("sjoin_point", df_point)
    eng_sedonadb.create_table_arrow("sjoin_polygon", df_polygon)
    eng_postgis.create_table_arrow("sjoin_point", df_point)
    eng_postgis.create_table_arrow("sjoin_polygon", df_polygon)

    sql = f"""
           SELECT sjoin_point.id id0, sjoin_polygon.id id1
           FROM sjoin_point {join_type} sjoin_polygon
           ON {on}
           ORDER BY id0, id1
           """

    sedonadb_results = eng_sedonadb.execute_and_collect(sql).to_pandas()
    assert len(sedonadb_results) > 0
    eng_postgis.assert_query_result(sql, sedonadb_results)
