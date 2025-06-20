import adbc_driver_manager.dbapi

import sedonadb.adbc


def connect(**kwargs) -> "Connection":
    """Connect to Sedona via ADBC."""
    db = None
    conn = None

    try:
        db = sedonadb.adbc.connect()
        conn = adbc_driver_manager.AdbcConnection(db)
        return adbc_driver_manager.dbapi.Connection(db, conn, **kwargs)
    except Exception:
        if conn:
            conn.close()
        if db:
            db.close()
        raise


Connection = adbc_driver_manager.dbapi.Connection
Cursor = adbc_driver_manager.dbapi.Cursor
