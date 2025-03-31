from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import IO, Any

import jsonlines
import orjson
from geoalchemy2 import functions as func
from sqlalchemy import Column, MetaData, select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ClauseElement

from schematools.exports import BaseExporter
from schematools.exports.csv import DatasetFieldSchema
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

    def geo_modifier(field: DatasetFieldSchema, column):
        if not field.is_geo:
            return column
        return func.ST_AsGeoJSON(func.ST_Transform(column, 4326)).label(field.db_name)

    def id_modifier(field: DatasetFieldSchema, column):
        if field.table.is_temporal and field.is_composite_key:
            return func.split_part(column, ".", 1).label(field.db_name)
        return column

    # We do not use the iso for datetime here, because json notation handles this.

    processors = (geo_modifier, id_modifier)

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
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ClauseElement | None,
        srid: str,
    ):
        writer = jsonlines.Writer(file_handle, dumps=_dumps)
        row_modifier = self._get_row_modifier(table)
        query = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        with self.connection.engine.execution_options(yield_per=1000).connect() as conn:
            result = conn.execute(query)
            for partition in result.mappings().partitions():
                for r in partition:
                    writer.write({toCamelCase(k): row_modifier[k](v) for k, v in r.items()})


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
