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


        # if table.main geo = relation
        if table.has_main_geometry and (rel_table := table.main_geometry_field.related_table):
                # Get the sa tables for current and related
                sa_table = self._get_sa_table(table)
                sa_related_table = self._get_sa_table(rel_table)

                # Get main geo sa column from related table
                related_geo_col = self._get_column(sa_related_table, rel_table.main_geometry_field)
                related_geo_col = related_geo_col.label("geometry")

                # Get all columns for query
                query_columns = list(columns)
                query_columns.append(related_geo_col)

                # Construct join on clause
                left_fk = getattr(sa_table.c, table.main_geometry_field.db_name)
                right_pk_field_id = rel_table.identifier[0]
                right_pk_field = rel_table.get_field_by_id(right_pk_field_id)
                right_pk = getattr(sa_related_table.c, right_pk_field.db_name)

                query = select(*query_columns).select_from(sa_table).join(
                    sa_related_table,
                    left_fk == right_pk,
                    isouter=True,
                )


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
