from sedonadb import _lib
from sedonadb.context import connect, configure_proj
from sedonadb import _options

options = _options.global_options()
"""Global options for SedonaDB"""

__version__ = _lib.sedona_python_version()

__all__ = ["connect", "options"]

# Attempt to configure PROJ on import. This will warn if PROJ
# can't be configured but should never error. The auto-configured
# value can be overridden as long as the call to configure_proj()
# occurs before actually creating a transform.
configure_proj("auto")
