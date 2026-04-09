from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import IO, Any

import jsonlines
import orjson
from sqlalchemy import Column, MetaData, select
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from schematools.exports.base import BaseExporter
from schematools.exports.modifiers import geo_modifier_geojson, id_modifier
from schematools.naming import toCamelCase
from schematools.types import DatasetTableSchema

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
    return orjson.dumps(obj, default=_default).decode("utf-8")


class JsonLinesExporter(BaseExporter):  # noqa: D101
    extension = "jsonl"

    # We do not use the iso for datetime here, because json notation handles this.

    processors = (geo_modifier_geojson, id_modifier)

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
        temporal_clause: ColumnElement[bool] | None,
        srid: str | None,
    ):
        writer = jsonlines.Writer(file_handle, dumps=_dumps)  # ty:ignore[unknown-argument]
        row_modifier = self._get_row_modifier(table)
        query: Select = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        with self.engine.execution_options(yield_per=1000).connect() as conn:
            result = conn.execute(query)
            for partition in result.mappings().partitions():
                for r in partition:
                    writer.write({toCamelCase(k): row_modifier[k](v) for k, v in r.items()})
