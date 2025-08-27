import pytest
from sedonadb.testing import geom_or_null, PostGIS, SedonaDB, val_or_null

# These tests should generally follow the same order as they are listed in rust/sedona-functions/src/register.rs


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        ("POINT (0 0)", None, None),
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", "POINT (0 0)", 0),
        (
            "POINT(-72.1235 42.3521)",
            "LINESTRING(-72.1260 42.45, -72.123 42.1546)",
            0.0015056772638228177,
        ),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            5.656854249492381,
        ),
    ],
)
def test_st_distance(eng, geom1, geom2, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_Distance({geom_or_null(geom1)}, {geom_or_null(geom2)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        # Currently geoarrow returns POINT (nan, nan) instead of POINT EMPTY
        ("POINT EMPTY", "POINT EMPTY", "POINT (nan nan)"),
        ("POINT (0 0)", "POINT (0 0)", "POINT (0 0)"),
        ("POINT (0 0)", "POINT (1 1)", "POINT (nan nan)"),
        ("POINT (0 0)", "LINESTRING (0 0, 1 1)", "POINT (0 0)"),
        ("LINESTRING (0 0, 1 1)", "LINESTRING (2 2, 3 3)", "LINESTRING EMPTY"),
        ("LINESTRING (0 0, 1 1)", "LINESTRING (1 1, 2 2)", "POINT (1 1)"),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            "POLYGON EMPTY",
        ),
        (
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
        ),
    ],
)
def test_st_intersection(eng, geom1, geom2, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_Intersection({geom_or_null(geom1)}, {geom_or_null(geom2)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        ("POINT (0 0)", None, None),
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", "POINT (0 0)", True),
        ("POINT EMPTY", "POINT (0 0)", False),
        ("POINT (0 0)", "LINESTRING (0 0, 1 1)", True),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "LINESTRING (0 0, 1 1)", True),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            False,
        ),
        (
            "POINT (1 1)",
            "GEOMETRYCOLLECTION (POINT (0 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), LINESTRING (0 0, 1 1))",
            True,
        ),
    ],
)
def test_st_intersects(eng, geom1, geom2, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_Intersects({geom_or_null(geom1)}, {geom_or_null(geom2)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT EMPTY", 0.0),
        ("LINESTRING EMPTY", 0.0),
        ("POLYGON EMPTY", 0.0),
        ("MULTIPOINT EMPTY", 0.0),
        ("MULTILINESTRING EMPTY", 0.0),
        ("MULTIPOLYGON EMPTY", 0.0),
        ("GEOMETRYCOLLECTION EMPTY", 0.0),
        ("POINT (5 2)", 0.0),
        ("MULTIPOINT ((0 0), (1 1))", 0.0),
        ("LINESTRING (0 0, 1 1)", 0.0),
        ("MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))", 0.0),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 1.0),
        (
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 1 0, 1 1, 0 1, 0 0)))",
            2.0,
        ),
        (
            "GEOMETRYCOLLECTION (POINT (0 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), LINESTRING (0 0, 1 1), GEOMETRYCOLLECTION (POLYGON ((0 0, -1 0, -1 -1, 0 -1, 0 0))))",
            2.0,
        ),
    ],
)
def test_st_area(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_Area({geom_or_null(geom)})", expected)


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT (0 0)", "POINT (0 0)"),
        ("LINESTRING (0 0, 1 1)", "POINT (0.5 0.5)"),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POINT (0.5 0.5)"),
        ("MULTIPOINT ((0 0), (1 1))", "POINT (0.5 0.5)"),
        ("MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))", "POINT (1 1)"),
        (
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 1 0, 1 1, 0 1, 0 0)))",
            "POINT (0.5 0.5)",
        ),
        (
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))",
            "POINT (0.5 0.5)",
        ),
        # Failing: issue with testing code: geoarrow-c rendering POINT (nan, nan)
        # instead of POINT EMPTY
        # https://github.com/geoarrow/geoarrow-c/issues/143
        ("POINT EMPTY", "POINT (nan nan)"),
        ("LINESTRING EMPTY", "POINT (nan nan)"),
        ("POLYGON EMPTY", "POINT (nan nan)"),
        ("MULTIPOINT EMPTY", "POINT (nan nan)"),
        ("MULTILINESTRING EMPTY", "POINT (nan nan)"),
        ("MULTIPOLYGON EMPTY", "POINT (nan nan)"),
        ("GEOMETRYCOLLECTION EMPTY", "POINT (nan nan)"),
    ],
)
def test_st_centroid(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_Centroid({geom_or_null(geom)})", expected)


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "distance", "expected"),
    [
        (None, "POINT (0 0)", 1.0, None),
        ("POINT (1 1)", None, 1.0, None),
        ("POINT (0 0)", "POINT (0 0)", None, None),
        (None, None, None, None),
        ("POINT (0 0)", "POINT (0 0)", 1.0, True),
        ("POINT (0 0)", "POINT (5 0)", 2.0, False),
        ("LINESTRING (0 0, 1 1)", "LINESTRING (2 2, 3 3)", 1.0, False),
        ("LINESTRING (0 0, 1 1)", "LINESTRING (10 0, 11 1)", 2.0, False),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            6.0,
            True,
        ),
        (
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1))",
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1))",
            1.0,
            True,
        ),
    ],
)
def test_st_dwithin(eng, geom1, geom2, distance, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_DWithin({geom_or_null(geom1)}, {geom_or_null(geom2)}, {val_or_null(distance)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT EMPTY", False),
        ("POINT Z EMPTY", True),
        ("POINT M EMPTY", False),
        ("POINT ZM EMPTY", True),
        ("POINT Z (0 0 0)", True),
        ("POINT M (0 0 0)", False),
        ("POINT ZM (0 0 0 0)", True),
        ("LINESTRING EMPTY", False),
        ("LINESTRING Z EMPTY", True),
        ("LINESTRING Z (0 0 0, 1 1 1)", True),
        ("POLYGON EMPTY", False),
        ("MULTIPOINT ((0 0), (1 1))", False),
        ("GEOMETRYCOLLECTION EMPTY", False),
        ("GEOMETRYCOLLECTION (POINT Z (0 0 0))", True),
        ("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT Z (0 0 0)))", True),
    ],
)
def test_st_hasz(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_HasZ({geom_or_null(geom)})", expected)


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT EMPTY", True),
        ("LINESTRING EMPTY", True),
        ("POLYGON EMPTY", True),
        ("MULTIPOINT EMPTY", True),
        ("MULTILINESTRING EMPTY", True),
        ("MULTIPOLYGON EMPTY", True),
        ("GEOMETRYCOLLECTION EMPTY", True),
        ("POINT (0 0)", False),
        ("LINESTRING (0 0, 1 1)", False),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", False),
        ("MULTIPOINT ((0 0), (1 1))", False),
        ("MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))", False),
        (
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 1 0, 1 1, 0 1, 0 0)))",
            False,
        ),
        (
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)))",
            False,
        ),
    ],
)
def test_st_isempty(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_IsEmpty({geom_or_null(geom)})", expected)


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT EMPTY", None),
        ("POINT Z EMPTY", None),
        ("POINT M EMPTY", None),
        ("POINT ZM EMPTY", None),
        ("POINT (1 2)", None),
        ("POINT Z (1 2 3)", None),
        ("POINT M (1 2 3)", 3),
        ("POINT ZM (1 2 3 -4)", -4),
    ],
)
def test_st_m(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_M({geom_or_null(geom)})", expected)


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom", "expected"),
    [
        (None, None),
        ("POINT EMPTY", None),
        ("POINT (1 2)", 1),
        ("POINT Z (1 2 3)", 1),
        ("POINT M (1 2 3)", 1),
        ("POINT ZM (1 2 3 4)", 1),
        ("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))", 0),
        ("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 0),
        (
            "GEOMETRYCOLLECTION (POINT (0 0), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), LINESTRING (0 0, 1 1))",
            0,
        ),
        (
            "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4), LINESTRING ZM (3 4 5 6, 7 8 9 10), POLYGON ZM ((0 0 0 0, 1 0 0 0, 0 1 0 0, 0 0 0 0)))",
            0,
        ),
        (
            "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6), GEOMETRYCOLLECTION (POINT (10 10)))",
            1,
        ),
    ],
)
def test_st_xmin(eng, geom, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(f"SELECT ST_XMin({geom_or_null(geom)})", expected)
