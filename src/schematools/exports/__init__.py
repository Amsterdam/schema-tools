import operator
from functools import reduce
from pathlib import Path
from tempfile import mkdtemp

from geoalchemy2 import functions as func  # ST_AsEWKT
from sqlalchemy import MetaData

from schematools.factories import tables_factory
from schematools.types import DatasetSchema

metadata = MetaData()


class BaseExporter:
    """Baseclass for exporting tables rows."""

    extension = ""

    def __init__(
        self,
        connection,
        dataset_schema: DatasetSchema,
        table_ids: list[str] | None = None,
        base_dir_str: str | None = None,
    ):
        self.connection = connection
        self.dataset_schema = dataset_schema
        self.table_ids = table_ids

        self.base_dir = Path(base_dir_str or mkdtemp())
        self.tables = (
            dataset_schema.tables
            if not table_ids
            else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
        )
        self.sa_tables = tables_factory(dataset_schema, metadata)

    def _get_scopes(self, table):
        """Return all the scopes involved in this table.

        User need all the scopes for this particular table to be able to download data.
        """
        return reduce(operator.or_, (f.auth for f in table.fields))

    def _get_column(self, sa_table, field):
        column = getattr(sa_table.c, field.db_name)
        processor = self.geo_modifier if field.is_geo else lambda col, fn: col
        return processor(column, field.db_name)

    def _get_columns(self, sa_table, table):

        for field in table.fields:
            try:
                yield self._get_column(sa_table, field)
            except AttributeError:
                pass  # skip unavailable columns

    def export_tables(self):
        for table in self.tables:
            srid = table.crs.split(":")[1] if table.crs else None
            if table.has_geometry_fields and srid is None:
                raise ValueError("Table has geo fields, but srid is None.")
            sa_table = self.sa_tables[table.id]
            with open(self.base_dir / f"{table.db_name}.{self.extension}", "w") as file_handle:
                self.write_rows(file_handle, table, sa_table, srid)

    def write_rows(self, file_handle, table, sa_table, srid):
        raise NotImplementedError
