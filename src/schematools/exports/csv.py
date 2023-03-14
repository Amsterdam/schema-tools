from __future__ import annotations

import csv

from geoalchemy2 import functions as func  # ST_AsEWKT
from sqlalchemy import MetaData, select

from schematools.exports import BaseExporter
from schematools.naming import toCamelCase

metadata = MetaData()


class CsvExporter(BaseExporter):  # noqa: D101
    extension = "csv"
    geo_modifier = staticmethod(lambda col, fn: func.ST_AsEWKT(col).label(fn))

    def write_rows(self, file_handle, table, sa_table, srid):  # noqa: D102
        columns = list(self._get_columns(sa_table, table))
        field_names = [c.name for c in columns]
        writer = csv.DictWriter(file_handle, field_names, extrasaction="ignore")
        writer.writerow({fn: toCamelCase(fn) for fn in field_names})
        query = select(self._get_columns(sa_table, table))
        if self.size is not None:
            query = query.limit(self.size)
        for r in self.connection.execute(query):
            writer.writerow(dict(r))


def export_csvs(connection, dataset_chema, output, table_ids, scopes, size):
    """Utility function to wrap the Exporter."""
    exporter = CsvExporter(connection, dataset_chema, output, table_ids, scopes, size)
    exporter.export_tables()
