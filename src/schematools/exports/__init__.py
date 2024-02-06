from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import IO, Iterable

import psycopg2
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ClauseElement

from schematools.factories import tables_factory
from schematools.types import _PUBLIC_SCOPE, DatasetFieldSchema, DatasetSchema, DatasetTableSchema

metadata = MetaData()


def enable_datetime_cast():
    """Register a special cast-to-string for the datetime type.

    This cast is not applied at module level, otherwise this
    cast would be enabled for all `schema-tools` code.
    """

    def cast_date(value, cur):
        if value is None:
            return None
        return str(value)

    # The bag_panden table had one record with a date (year = 0001)
    # that lets the dbapi `cursor.fetchmany` crash.
    # We need to define a custom type to handle this error,
    # because the historical record with this particular date cannot
    # be modified.
    date_oid = (1184,)  # This oid. can be obtained from cursor.description
    TIMESTAMPSTR = psycopg2.extensions.new_type(date_oid, "TIMESTAMP", cast_date)
    psycopg2.extensions.register_type(TIMESTAMPSTR)


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
        temporal_date: date = date.today(),
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
        self.temporal_date = temporal_date

        self.base_dir = Path(output)
        self.tables = (
            dataset_schema.tables
            if not table_ids
            else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
        )
        self.sa_tables = tables_factory(dataset_schema, metadata)

    def _get_column(self, sa_table: Table, field: DatasetFieldSchema) -> Column:
        column = getattr(sa_table.c, field.db_name)
        # apply all processors
        for processor in self.processors:
            column = processor(field, column)

        return column

    def _get_columns(self, sa_table: Table, table: DatasetTableSchema) -> Iterable[Column]:
        for field in _get_fields(self.dataset_schema, table, self.scopes):
            try:
                yield self._get_column(sa_table, field)
            except AttributeError:
                pass  # skip unavailable columns

    def _get_temporal_clause(
        self, sa_table: Table, table: DatasetTableSchema
    ) -> ClauseElement | None:
        if not table.is_temporal:
            return None
        temporal = table.temporal
        for dimension in temporal.dimensions.values():
            start = getattr(sa_table.c, dimension.start.db_name)
            end = getattr(sa_table.c, dimension.end.db_name)
            return (start <= self.temporal_date) & ((end > self.temporal_date) | (end == None))
        return None

    def export_tables(self):
        for table in self.tables:
            srid = table.crs.split(":")[1] if table.crs else None
            if table.has_geometry_fields and srid is None:
                raise ValueError("Table has geo fields, but srid is None.")
            sa_table = self.sa_tables[table.id]
            columns = list(self._get_columns(sa_table, table))
            if not columns:
                return
            with open(
                self.base_dir / f"{table.db_name}.{self.extension}", "w", encoding="utf8"
            ) as file_handle:
                self.write_rows(
                    file_handle,
                    table,
                    columns,
                    self._get_temporal_clause(sa_table, table),
                    srid,
                )

    def write_rows(  # noqa: D102
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ClauseElement | None,
        srid: str,
    ):
        raise NotImplementedError


def _get_fields(dataset_schema: DatasetSchema, table: DatasetTableSchema, scopes: list[str]):
    parent_scopes = set(dataset_schema.auth | table.auth) - {_PUBLIC_SCOPE}
    for field in table.fields:
        if field.is_array:
            continue
        if field.is_internal:
            continue
        if parent_scopes | set(field.auth) - {_PUBLIC_SCOPE} <= set(scopes):
            yield field
