from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import IO, Any

import orjson
from sqlalchemy import Column, MetaData, select
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
    return orjson.dumps(obj, default=_default).decode("utf-8")


class GeoJsonExporter(BaseExporter):
    extension = "geojson"

    processors = (geo_modifier_geojson, id_modifier)

    def write_rows(
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ColumnElement[bool] | None,
        srid: str | None,
    ):
        query = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        try:
            # Write header
            file_handle.write('{"type": "FeatureCollection", "features": [')

            first_feature = True
            with (
                self.engine.execution_options(
                    stream_results=True, max_row_buffer=1000
                ).connect() as connection,
                connection.execute(query) as result,
            ):
                for partition in result.mappings().partitions(size=1000):
                    for row in partition:
                        try:
                            properties = {}
                            geometry = None
                            for k, v in row.items():
                                if (isinstance(v, str) and v.startswith('{"type":')) or (
                                    isinstance(v, bytes) and v.startswith(b'{"type":')
                                ):
                                    try:
                                        geometry = orjson.loads(v)
                                        if (
                                            not isinstance(geometry, dict)
                                            or "type" not in geometry
                                        ):
                                            continue  # Skip invalid geometry
                                    except orjson.JSONDecodeError:
                                        continue  # Skip invalid JSON
                                else:
                                    properties[toCamelCase(k)] = v

                            if geometry:
                                feature = {
                                    "type": "Feature",
                                    "properties": properties,
                                    "geometry": geometry,
                                }
                                try:
                                    serialized_feature = _dumps(feature)
                                    # Only write comma and feature if serialization succeeded
                                    if not first_feature:
                                        file_handle.write(",")
                                    file_handle.write(serialized_feature)
                                    first_feature = False
                                except (TypeError, UnicodeDecodeError):
                                    continue  # Skip features that can't be serialized
                        except OSError:
                            raise  # Re-raise file writing errors

            file_handle.write("]}")
        except OSError as e:
            raise OSError(f"Failed to write GeoJSON file: {e!s}") from e

    def _process_row(self, row, features):
        """Process a single row and add it to features if it contains geometry."""
        properties = {}
        geometry = None
        for k, v in row.items():
            if (isinstance(v, str) and v.startswith('{"type":')) or (
                isinstance(v, bytes) and v.startswith(b'{"type":')
            ):
                geometry = orjson.loads(v)
            else:
                properties[toCamelCase(k)] = v

        if geometry:
            feature = {"type": "Feature", "properties": properties, "geometry": geometry}
            features.append(feature)
