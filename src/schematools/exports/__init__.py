from __future__ import annotations

from pathlib import Path
from typing import IO

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Connection

from schematools.factories import tables_factory
from schematools.types import _PUBLIC_SCOPE, DatasetFieldSchema, DatasetSchema, DatasetTableSchema

metadata = MetaData()


class BaseExporter:
    """Baseclass for exporting tables rows."""

    extension = ""

    def __init__(
        self,
        connection: Connection,
        dataset_schema: DatasetSchema,
        output: str,
        table_ids: list[str] | None = None,
        scopes: list[str] | None = None,
        size: int | None = None,
    ):
        """Constructor.

        Args:
        connection: SQLAlchemy connection object.
        dataset_schema: Schema that needs export as geopackageself.
        output: path on the filesystem where output should be storedself.
        table_ids: optional parameter for a subset for the tables of the datasetself.
        scopes: Keycloak scopes that need to be taken into accountself.
            The geopackage will be produced contains information that is only
            accessible with these scopes.
        size: To produce a subset of the rows, mainly for testing.
        """
        self.connection = connection
        self.dataset_schema = dataset_schema
        self.table_ids = table_ids
        self.scopes = set(scopes)
        self.size = size

        self.base_dir = Path(output)
        self.tables = (
            dataset_schema.tables
            if not table_ids
            else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
        )
        self.sa_tables = tables_factory(dataset_schema, metadata)

    def _get_column(self, sa_table: Table, field: DatasetFieldSchema):
        column = getattr(sa_table.c, field.db_name)
        processor = self.geo_modifier if field.is_geo else lambda col, _fn: col
        return processor(column, field.db_name)

    def _get_columns(self, sa_table: Table, table: DatasetTableSchema):
        for field in _get_fields(self.dataset_schema, table, self.scopes):
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

    def write_rows(
        self, file_handle: IO[str], table: DatasetTableSchema, sa_table: Table, srid: str
    ):
        raise NotImplementedError


def _get_fields(dataset_schema: DatasetSchema, table: DatasetTableSchema, scopes: list[str]):
    parent_scopes = set(dataset_schema.auth | table.auth) - {_PUBLIC_SCOPE}
    for field in table.fields:
        if field.is_array:
            continue
        if parent_scopes | set(field.auth) - {_PUBLIC_SCOPE} <= set(scopes):
            yield field
