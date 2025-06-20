from sedonadb import _lib
from sedonadb.context import connect

__version__ = _lib.sedona_python_version()

__all__ = ["connect"]
