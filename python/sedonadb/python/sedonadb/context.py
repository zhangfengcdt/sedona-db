from pathlib import Path
from typing import Union, Iterable

from sedonadb._lib import InternalContext
from sedonadb.dataframe import DataFrame


class SedonaContext:
    """Context for executing queries using Sedona

    This object keeps track of state such as registered functions,
    registered tables, and available memory. This is similar to a
    Spark SessionContext or a database connection.
    """

    def __init__(self):
        self._impl = InternalContext()

    def read_parquet(self, table_paths: Union[str, Path, Iterable[str]]) -> DataFrame:
        """Create a [`DataFrame`][] from one or more Parquet files

        Args:
            table_paths: A str, Path, or iterable of paths containing URLs to Parquet
                files.

        Examples:

            ```python
            >>> import sedonadb
            >>> url = "https://github.com/apache/sedona-testing/raw/refs/heads/main/data/parquet/geoparquet-1.1.0.parquet"
            >>> sedonadb.connect().read_parquet(url)
            <sedonadb.dataframe.DataFrame object at ...>

            ```
        """
        if isinstance(table_paths, (str, Path)):
            table_paths = [table_paths]

        return DataFrame(self._impl.read_parquet([str(path) for path in table_paths]))

    def sql(self, sql: str) -> DataFrame:
        """Create a [`DataFrame`][] by executing SQL

        Parses a SQL string into a logical plan and returns a DataFrame
        that can be used to request results or further modify the query.

        Args:
            sql: A single SQL statement.

        Examples:

            ```python
            >>> import sedonadb
            >>> sedonadb.connect().sql("SELECT ST_Point(0, 1) as geom")
            <sedonadb.dataframe.DataFrame object at ...>

            ```
        """
        return DataFrame(self._impl.sql(sql))


def connect() -> SedonaContext:
    """Create a new [`SedonaContext`][]"""
    return SedonaContext()
