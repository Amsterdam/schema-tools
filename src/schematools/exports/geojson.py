from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import IO, Any

import orjson
from geoalchemy2 import functions as func
from sqlalchemy import Column, MetaData, select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ClauseElement

from schematools.exports import BaseExporter
from schematools.naming import toCamelCase
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema

metadata = MetaData()


def _default(obj: Any) -> str:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError


def _dumps(obj: Any) -> str:
    return orjson.dumps(obj, default=_default)


class GeoJsonExporter(BaseExporter):
    extension = "geojson"

    def geo_modifier(field: DatasetFieldSchema, column):
        if not field.is_geo:
            return column
        return func.ST_AsGeoJSON(func.ST_Transform(column, 4326)).label(field.db_name)

    def id_modifier(field: DatasetFieldSchema, column):
        if field.table.is_temporal and field.is_composite_key:
            return func.split_part(column, ".", 1).label(field.db_name)
        return column

    processors = (geo_modifier, id_modifier)

    def write_rows(
        self,
        file_handle: IO[str],
        table: DatasetTableSchema,
        columns: Iterable[Column],
        temporal_clause: ClauseElement | None,
        srid: str,
    ):
        features = []
        query = select(*columns)
        if temporal_clause is not None:
            query = query.where(temporal_clause)
        if self.size is not None:
            query = query.limit(self.size)

        with self.connection.engine.execution_options(yield_per=1000).connect() as conn:
            result = conn.execute(query)
            for partition in result.mappings().partitions():
                for row in partition:
                    self._process_row(row, features)

            geojson = {"type": "FeatureCollection", "features": features}
            file_handle.write(_dumps(geojson).decode())

    def _process_row(self, row, features):
        """Process a single row and add it to features if it contains geometry."""
        properties = {}
        geometry = None
        for k, v in row.items():
            if isinstance(v, (str, bytes)) and v.startswith('{"type":'):
                geometry = orjson.loads(v)
            else:
                properties[toCamelCase(k)] = v

        if geometry:
            feature = {"type": "Feature", "properties": properties, "geometry": geometry}
            features.append(feature)


def export_geojsons(
    connection: Connection,
    dataset_schema: DatasetSchema,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
):
    exporter = GeoJsonExporter(connection, dataset_schema, output, table_ids, scopes, size)
    exporter.export_tables()
