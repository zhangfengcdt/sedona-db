from pathlib import Path
from typing import Union, Iterable

from sedonadb._lib import InternalContext
from sedonadb.dataframe import DataFrame, _create_data_frame


class SedonaContext:
    """Context for executing queries using Sedona

    This object keeps track of state such as registered functions,
    registered tables, and available memory. This is similar to a
    Spark SessionContext or a database connection.
    """

    def __init__(self):
        self._impl = InternalContext()

    def create_data_frame(self, obj, schema=None) -> DataFrame:
        return _create_data_frame(self._impl, obj, schema)

    def view(self, name: str) -> DataFrame:
        """Create a [DataFrame][sedonadb.dataframe.DataFrame] from a named view

        Refer to a named view registered with this context.

        Args:
            name: The name of the view

        Examples:

            >>> import sedonadb
            >>> con = sedonadb.connect()
            >>> con.sql("SELECT ST_Point(0, 1) as geom").to_view("foofy")
            >>> con.view("foofy").show()
            ┌────────────┐
            │    geom    │
            │     wkb    │
            ╞════════════╡
            │ POINT(0 1) │
            └────────────┘
            >>> con.drop_view("foofy")

        """
        return DataFrame(self._impl, self._impl.view(name))

    def drop_view(self, name: str) -> None:
        """Remove a named view

        Args:
            name: The name of the view

        Examples:

            >>> import sedonadb
            >>> con = sedonadb.connect()
            >>> con.sql("SELECT ST_Point(0, 1) as geom").to_view("foofy")
            >>> con.drop_view("foofy")

        """
        self._impl.drop_view(name)

    def read_parquet(self, table_paths: Union[str, Path, Iterable[str]]) -> DataFrame:
        """Create a [DataFrame][sedonadb.dataframe.DataFrame] from one or more Parquet files

        Args:
            table_paths: A str, Path, or iterable of paths containing URLs to Parquet
                files.

        Examples:

            >>> import sedonadb
            >>> url = "https://github.com/apache/sedona-testing/raw/refs/heads/main/data/parquet/geoparquet-1.1.0.parquet"
            >>> sedonadb.connect().read_parquet(url)
            <sedonadb.dataframe.DataFrame object at ...>

        """
        if isinstance(table_paths, (str, Path)):
            table_paths = [table_paths]

        return DataFrame(
            self._impl, self._impl.read_parquet([str(path) for path in table_paths])
        )

    def sql(self, sql: str) -> DataFrame:
        """Create a [DataFrame][sedonadb.dataframe.DataFrame] by executing SQL

        Parses a SQL string into a logical plan and returns a DataFrame
        that can be used to request results or further modify the query.

        Args:
            sql: A single SQL statement.

        Examples:

            >>> import sedonadb
            >>> sedonadb.connect().sql("SELECT ST_Point(0, 1) as geom")
            <sedonadb.dataframe.DataFrame object at ...>

        """
        return DataFrame(self._impl, self._impl.sql(sql))


def connect() -> SedonaContext:
    """Create a new [SedonaContext][sedonadb.context.SedonaContext]"""
    return SedonaContext()
