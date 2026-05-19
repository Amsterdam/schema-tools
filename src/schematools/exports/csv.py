from __future__ import annotations

import csv
from collections.abc import Iterable
from typing import IO

from sqlalchemy import Column, MetaData, select
from sqlalchemy.sql.elements import ColumnElement

from schematools.exports.base import BaseExporter
from schematools.exports.modifiers import datetime_modifier, geo_modifier_ewkt, id_modifier
from schematools.naming import to_snake_case, toCamelCase
from schematools.types import DatasetTableSchema

metadata = MetaData()


class CsvExporter(BaseExporter):  # noqa: D101
    extension = "csv"
    processors = (geo_modifier_ewkt, id_modifier, datetime_modifier)

    def write_rows(
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ColumnElement[bool] | None,
        srid: str | None,
    ):
        field_names = [c.name for c in columns]
        writer = csv.DictWriter(file_handle, field_names, extrasaction="ignore")
        # Use capitalize() on headers, because csv export does the same
        writer.writerow({fn: toCamelCase(fn).capitalize() for fn in field_names})

        array_string_fields = {
            to_snake_case(field.id)
            for field in table.fields
            if (field.is_array and field.get("items", {}).get("type") == "string")
        }
        query = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        # Use server-side cursor with small batches
        with (
            self.engine.execution_options(
                stream_results=True, max_row_buffer=1000
            ).connect() as connection,
            connection.execute(query) as result,
        ):
            for partition in result.mappings().partitions(size=1000):
                writer.writerows(
                    {
                        key: (
                            ",".join(map(str, value))
                            if key in array_string_fields and isinstance(value, list)
                            else value
                        )
                        for key, value in dict(row).items()
                    }
                    for row in partition
                )
