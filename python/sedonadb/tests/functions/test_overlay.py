import pytest
from sedonadb.testing import geom_or_null, PostGIS, SedonaDB


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", None, None),
        # Currently geoarrow returns POINT (nan, nan) instead of POINT EMPTY
        ("POINT EMPTY", "POINT EMPTY", "POINT (nan nan)"),
        ("POINT (0 0)", "POINT (0 0)", "POINT (nan nan)"),
        ("POINT (0 0)", "LINESTRING (0 0, 1 1)", "POINT (nan nan)"),
        ("POINT (0 0)", "POINT (1 1)", "POINT (0 0)"),
        (
            "LINESTRING (0 0, 1 1)",
            "LINESTRING (0.5 0.5, 1 1)",
            "LINESTRING (0 0, 0.5 0.5)",
        ),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            "POLYGON ((0 1, 1 1, 1 0, 0 0, 0 1))",
        ),
        (
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 2 2))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "LINESTRING (1 1, 2 2)",
        ),
    ],
)
def test_st_difference(eng, geom1, geom2, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_Difference({geom_or_null(geom1)}, {geom_or_null(geom2)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", None, None),
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
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", None, None),
        # Currently geoarrow returns POINT (nan, nan) instead of POINT EMPTY
        ("POINT EMPTY", "POINT EMPTY", "POINT (nan nan)"),
        ("POINT (0 0)", "POINT (0 0)", "POINT (nan nan)"),
        ("POINT (0 0)", "LINESTRING (0 0, 1 1)", "LINESTRING (0 0, 1 1)"),
        ("POINT (0 0)", "POINT (1 1)", "MULTIPOINT (0 0, 1 1)"),
        (
            "LINESTRING (0 0, 1 1)",
            "LINESTRING (0.5 0.5, 1 1)",
            "LINESTRING (0 0, 0.5 0.5)",
        ),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))",
        ),
        (
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 2 2))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "LINESTRING (1 1, 2 2)",
        ),
    ],
)
def test_st_symdifference(eng, geom1, geom2, expected):
    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_SymDifference({geom_or_null(geom1)}, {geom_or_null(geom2)})",
        expected,
    )


@pytest.mark.parametrize("eng", [SedonaDB, PostGIS])
@pytest.mark.parametrize(
    ("geom1", "geom2", "expected"),
    [
        (None, None, None),
        (None, "POINT (0 0)", None),
        ("POINT (0 0)", None, None),
        ("POINT (0 0)", "POINT (0 0)", "POINT (0 0)"),
        ("POINT (0 0)", "LINESTRING (0 0, 1 1)", "LINESTRING (0 0, 1 1)"),
        ("POINT (0 0)", "POINT (1 1)", "MULTIPOINT (0 0, 1 1)"),
        (
            "LINESTRING (0 0, 1 1)",
            "LINESTRING (0.5 0.5, 1 1)",
            "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1))",
        ),
        (
            "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))",
            "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))",
        ),
        (
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
            "GEOMETRYCOLLECTION (POINT (-1 0), LINESTRING (0 0, 1 1))",
        ),
    ],
)
def test_st_union(eng, geom1, geom2, expected):
    # PostGIS fails to interpret NULL properly for union, so we explicitly cast it for PostGIS
    arg1 = geom_or_null(geom1)
    arg2 = geom_or_null(geom2)
    if eng == PostGIS:
        if geom1 is None:
            arg1 = "NULL::geometry"
        if geom2 is None:
            arg2 = "NULL::geometry"

    eng = eng.create_or_skip()
    eng.assert_query_result(
        f"SELECT ST_Union({arg1}, {arg2})",
        expected,
    )
