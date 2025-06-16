from sedona_rs import _lib
from sedona_rs.context import connect

__version__ = _lib.sedona_python_version()

__all__ = ["connect"]
