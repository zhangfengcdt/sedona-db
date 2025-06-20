import adbc_driver_manager

from sedonadb import _lib


def connect() -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC connection to Sedona."""
    return adbc_driver_manager.AdbcDatabase(
        driver=_lib.__file__, entrypoint="AdbcSedonadbDriverInit"
    )
