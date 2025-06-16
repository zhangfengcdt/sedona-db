from sedona_rs._lib import InternalContext
from sedona_rs.dataframe import DataFrame


class SedonaContext:
    """Context for executing queries using Sedona

    This object keeps track of state such as registered functions,
    registered tables, and available memory. This is similar to a
    Spark SessionContext or a database connection.
    """

    def __init__(self):
        self._impl = InternalContext()

    def sql(self, sql: str) -> DataFrame:
        """Create a [`DataFrame`][] by executing SQL

        Parses a SQL string into a logical plan and returns a DataFrame
        that can be used to request results or further modify the query.

        Args:
            sql: A single SQL statement.

        Examples:

            ```python
            >>> import sedona_rs
            >>> sedona_rs.connect().sql("SELECT ST_Point(0, 1) as geom")
            <sedona_rs.dataframe.DataFrame object at ...>

            ```
        """
        return DataFrame(self._impl.sql(sql))


def connect() -> SedonaContext:
    """Create a new [`SedonaContext`][]"""
    return SedonaContext()
