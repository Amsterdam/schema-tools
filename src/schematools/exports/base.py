from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import IO

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.sql.elements import ColumnElement

from schematools.factories import tables_factory
from schematools.types import (
    _PUBLIC_SCOPE,
    DatasetFieldSchema,
    DatasetTableSchema,
    ExportContext,
    Scope,
)

metadata = MetaData()

logger = logging.getLogger(__name__)


class BaseExporter:
    """Baseclass for exporting tables rows."""

    extension = ""
    processors: Iterable[Callable] = ()

    def __init__(self, context: ExportContext):
        """Constructor.

        Args:
        connection: SQLAlchemy connection object.
        dataset_schema: Schema that needs export as geopackage.
        output: path on the filesystem where output should be stored.
        table_ids: optional parameter for a subset for the tables of the dataset.
        scopes: Keycloak scopes that need to be taken into account.
            The geopackage will be produced contains information that is only
            accessible with these scopes.
        size: To produce a subset of the rows, mainly for testing.
        """
        self.connection = context.connection
        self.dataset_schema = context.dataset
        self.export = context.export
        self.scopes = context.export.scopes
        self.size = context.size
        self.temporal_date = context.temporal_date or datetime.now().astimezone()

        self.base_dir = context.folder
        self.base_dir.mkdir(exist_ok=True)
        self.tables = context.export.tables
        self.sa_tables = tables_factory(
            self.dataset_schema, metadata, version=context.export.version
        )

    def _get_fields(self, table: DatasetTableSchema):
        dataset = self.dataset_schema
        public_scope = Scope.from_string(_PUBLIC_SCOPE)
        parent_scopes = set(dataset.scopes | table.scopes) - {public_scope}
        for field in table.fields:
            if field.is_array and not (self.extension == "geojson" or self.extension == "jsonl"):
                continue
            if field.is_internal:
                continue
            if parent_scopes | set(field.scopes) - {public_scope} <= self.scopes:
                if field.is_object and not field.is_relation:
                    yield from field.subfields
                else:
                    yield field

    def _get_column(self, sa_table: Table, field: DatasetFieldSchema) -> Column:
        column = getattr(sa_table.c, field.db_name)
        # apply all processors
        for processor in self.processors:
            column = processor(field, column)

        return column

    def _get_columns(self, sa_table: Table, table: DatasetTableSchema) -> Iterable[Column]:
        for field in self._get_fields(table):
            try:
                yield self._get_column(sa_table, field)
            except AttributeError:
                pass  # skip unavailable columns

    def _get_temporal_clause(
        self, sa_table: Table, table: DatasetTableSchema
    ) -> ColumnElement[bool] | None:
        if not table.is_temporal or not table.temporal:
            return None
        temporal = table.temporal
        for dimension in temporal.dimensions.values():
            start: Column = getattr(sa_table.c, dimension.start.db_name)
            end: Column = getattr(sa_table.c, dimension.end.db_name)
            return (
                # This is an SQLAlchemy statement, hence the &, | and == operators:
                (start <= self.temporal_date)
                & ((end > self.temporal_date) | (end == None))  # noqa: E711
            )
        return None

    def export_tables(self):
        for table in self.tables:
            srid = table.crs.split(":")[1] if table.crs else None
            if table.has_geometry_fields and srid is None:
                raise ValueError("Table has geo fields, but srid is None.")
            sa_table = self.sa_tables[table.id]
            columns = list(self._get_columns(sa_table, table))
            if not columns:
                continue
            path = self.base_dir / self.export.table_filename(table.id)
            if path.exists() and path.stat().st_size > 0:
                logger.warning("File %s already exists. It will be skipped.", path.name)
                continue
            logger.info("Exporting %s.", path.name)
            path.touch()
            with path.open("w", encoding="utf8") as file_handle:
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
        temporal_clause: ColumnElement[bool] | None,
        srid: str | None,
    ):
        raise NotImplementedError
