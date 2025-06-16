import sedona_rs.dbapi


def test_query():
    con = sedona_rs.dbapi.connect()
    with con.cursor() as cur:
        cur.execute("SELECT ST_AsText(ST_GeomFromWKT('POINT (0 1)'))")
        assert cur.fetchone() == ("POINT(0 1)",)
