from __future__ import annotations

import csv
from collections.abc import Iterable
from datetime import datetime
from typing import IO

from geoalchemy2 import functions as gfunc  # ST_AsEWKT
from sqlalchemy import Column, MetaData, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ClauseElement

from schematools.exports import BaseExporter
from schematools.naming import toCamelCase
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema

metadata = MetaData()


class CsvExporter(BaseExporter):  # noqa: D101
    extension = "csv"

    def geo_modifier(field: DatasetFieldSchema, column):
        if not field.is_geo:
            return column
        return gfunc.ST_AsEWKT(column).label(field.db_name)

    def id_modifier(field: DatasetFieldSchema, column):
        # We need an extra check for old relation definitions
        # that are defined as plain strings in the amsterdam schema
        # and not as objects.
        if field.table.is_temporal and (
            field.is_composite_key
            or field.related_table is not None
            and field.related_table.is_temporal
        ):
            return func.split_part(column, ".", 1).label(field.db_name)
        return column

    def datetime_modifier(field: DatasetFieldSchema, column):
        if field.type == "string" and field.format == "date-time":
            return func.to_char(column, 'YYYY-MM-DD"T"HH24:MI:SS').label(field.db_name)
        return column

    processors = (geo_modifier, id_modifier, datetime_modifier)

    def write_rows(
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ClauseElement | None,
        srid: str,
    ):
        field_names = [c.name for c in columns]
        writer = csv.DictWriter(file_handle, field_names, extrasaction="ignore")
        # Use capitalize() on headers, because csv export does the same
        writer.writerow({fn: toCamelCase(fn).capitalize() for fn in field_names})

        query = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        # Use server-side cursor with small batches
        with self.connection.execution_options(stream_results=True, max_row_buffer=1000).execute(
            query
        ) as result:
            for partition in result.mappings().partitions(size=1000):
                writer.writerows(partition)


def export_csvs(
    connection: Connection,
    dataset_schema: DatasetSchema,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
    temporal_date: datetime | None = None,
):
    """Utility function to wrap the Exporter."""
    exporter = CsvExporter(
        connection, dataset_schema, output, table_ids, scopes, size, temporal_date
    )
    exporter.export_tables()
