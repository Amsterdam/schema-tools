from __future__ import annotations

import csv
from typing import IO

from geoalchemy2 import functions as func  # ST_AsEWKT
from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Connection

from schematools.exports import BaseExporter
from schematools.naming import toCamelCase
from schematools.types import DatasetSchema

metadata = MetaData()


class CsvExporter(BaseExporter):  # noqa: D101
    extension = "csv"
    geo_modifier = staticmethod(lambda col, fn: func.ST_AsEWKT(col).label(fn))

    def write_rows(  # noqa: D102
        self, file_handle: IO[str], table: DatasetSchema, sa_table: Table, srid: str
    ):
        columns = list(self._get_columns(sa_table, table))
        field_names = [c.name for c in columns]
        writer = csv.DictWriter(file_handle, field_names, extrasaction="ignore")
        writer.writerow({fn: toCamelCase(fn) for fn in field_names})
        query = select(self._get_columns(sa_table, table))
        if self.size is not None:
            query = query.limit(self.size)
        result = self.connection.execution_options(yield_per=1000).execute(query)
        for partition in result.partitions():
            for r in partition:
                writer.writerow(dict(r))


def export_csvs(
    connection: Connection,
    dataset_schema: DatasetSchema,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
):
    """Utility function to wrap the Exporter."""
    exporter = CsvExporter(connection, dataset_schema, output, table_ids, scopes, size)
    exporter.export_tables()
