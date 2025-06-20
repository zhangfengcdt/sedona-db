from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    import pandas
    import geopandas
    import pyarrow


class DataFrame:
    """Representation of a (lazy) collection of columns

    This object is usually constructed from a [`SedonaContext`][] by
    importing an object, reading a file, or executing SQL.
    """

    def __init__(self, impl):
        self._impl = impl

    def __arrow_c_schema__(self):
        """ArrowSchema PyCapsule interface

        Returns a PyCapsule wrapping an Arrow C Schema for interoperability
        with libraries that understand Arrow C data types. See the
        [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
        for more details.
        """
        return self._impl.__arrow_c_schema__()

    def __arrow_c_stream__(self, requested_schema=None):
        """ArrowArrayStream Stream PyCapsule interface

        Returns a PyCapsule wrapping an Arrow C ArrayStream for interoperability
        with libraries that understand Arrow C data types. See the
        [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
        for more details.

        Args:
            requested_schema: A PyCapsule representing the desired output schema.
        """
        return self._impl.__arrow_c_stream__(requested_schema=requested_schema)

    def to_arrow_table(self, schema=None) -> "pyarrow.Table":
        """Execute and collect results as a PyArrow Table

        Executes the logical plan represented by this object and returns a
        PyArrow Table. This requires that pyarrow is installed.

        Args:
            schema: The requested output schema or `None` to use the inferred
                schema.

        Examples:

            ```python
            >>> import sedonadb
            >>> con = sedonadb.connect()
            >>> con.sql("SELECT ST_Point(0, 1) as geometry").to_arrow_table()
            pyarrow.Table
            geometry: extension<geoarrow.wkb<WkbType>>
            ----
            geometry: [[01010000000000000000000000000000000000F03F]]

            ```
        """
        import pyarrow as pa
        import geoarrow.pyarrow  # noqa: F401

        if schema is None:
            return pa.table(self)
        else:
            return pa.table(self, schema=pa.schema(schema))

    def to_pandas(
        self, geometry: Optional[str] = None
    ) -> Union["pandas.DataFrame", "geopandas.GeoDataFrame"]:
        """Execute and collect results as a pandas DataFrame or GeoDataFrame

        If this data frame contains geometry columns, collect results as a
        single [`geopandas.GeoDataFrame`][]. Otherwise, collect results as a
        [`pandas.DataFrame`][].

        Args:
            geometry: If specified, the name of the column to use for the default
                geometry column. If not specified, this is inferred as the column
                named "geometry", the column named "geography", or the first
                column with a spatial data type (in that order).

        Examples:

            ```python
            >>> import sedonadb
            >>> con = sedonadb.connect()
            >>> con.sql("SELECT ST_Point(0, 1) as geometry").to_pandas()
                  geometry
            0  POINT (0 1)

            ```
        """
        table = self.to_arrow_table()

        if geometry is None:
            geometry = self._impl.primary_geometry_column()

        if geometry:
            from geopandas import GeoDataFrame

            return GeoDataFrame.from_arrow(table, geometry=geometry)
        else:
            return table.to_pandas()
