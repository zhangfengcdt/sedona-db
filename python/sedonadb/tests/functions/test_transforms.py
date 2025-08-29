import pytest
from sedonadb.testing import PostGIS, SedonaDB


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
def test_st_transform(eng):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        "SELECT ST_Transform(ST_GeomFromText('POINT (1 1)'), 'EPSG:4326', 'EPSG:3857')",
        "POINT (111319.490793274 111325.142866385)",
        wkt_precision=9,
    )
