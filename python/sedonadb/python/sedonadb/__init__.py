from sedonadb import _lib
from sedonadb.context import connect
from sedonadb import _options

options = _options.global_options()
"""Global options for SedonaDB"""

__version__ = _lib.sedona_python_version()

__all__ = ["connect", "options"]
