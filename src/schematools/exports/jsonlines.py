from __future__ import annotations

from decimal import Decimal
from typing import IO, Any

import jsonlines
import orjson
from geoalchemy2 import functions as func
from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Connection

from schematools.exports import BaseExporter
from schematools.naming import toCamelCase
from schematools.types import DatasetSchema, DatasetTableSchema

metadata = MetaData()


def _default(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


def _dumps(obj: Any) -> str:
    """Json serializer.

    Unfortunately, orjson does not support Decimal serialization,
    so we need this extra function.
    """
    return orjson.dumps(obj, default=_default)


class JsonLinesExporter(BaseExporter):  # noqa: D101
    extension = "jsonl"
    geo_modifier = staticmethod(
        lambda col, fn: func.ST_AsGeoJSON(func.ST_Transform(col, 4326)).label(fn)
    )

    def _get_row_modifier(self, table: DatasetTableSchema):
        lookup = {}
        for field in table.fields:
            lookup[field.db_name] = (
                (lambda v: orjson.loads(v) if v else v)
                if field.is_geo or field.is_nested_object
                else lambda v: v
            )
        return lookup

    def write_rows(  # noqa: D102
        self, file_handle: IO[str], table: DatasetTableSchema, sa_table: Table, srid: str
    ):
        writer = jsonlines.Writer(file_handle, dumps=_dumps)
        row_modifier = self._get_row_modifier(table)
        query = select(self._get_columns(sa_table, table))
        if self.size is not None:
            query = query.limit(self.size)
        result = self.connection.execution_options(yield_per=1000).execute(query)
        for partition in result.partitions():
            for r in partition:
                writer.write({toCamelCase(k): row_modifier[k](v) for k, v in dict(r).items()})


def export_jsonls(
    connection: Connection,
    dataset_schema: DatasetSchema,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
):
    """Utility function to wrap the Exporter."""
    exporter = JsonLinesExporter(connection, dataset_schema, output, table_ids, scopes, size)
    exporter.export_tables()
