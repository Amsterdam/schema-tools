from __future__ import annotations

from geoalchemy2 import functions as gfunc  # ST_AsEWKT
from sqlalchemy import func

from schematools.types import DatasetFieldSchema


def geo_modifier_ewkt(field: DatasetFieldSchema, column):
    if not field.is_geo:
        return column
    return gfunc.ST_AsEWKT(column).label(field.db_name)


def geo_modifier_geojson(field: DatasetFieldSchema, column):
    if not field.is_geo:
        return column
    return func.ST_AsGeoJSON(func.ST_Transform(column, 4326)).label(field.db_name)


def id_modifier(field: DatasetFieldSchema, column):
    # We need an extra check for old relation definitions
    # that are defined as plain strings in the amsterdam schema
    # and not as objects.
    if (
        field.table
        and field.table.is_temporal
        and (
            field.is_composite_key
            or field.related_table is not None
            and field.related_table.is_temporal
        )
    ):
        return func.split_part(column, ".", 1).label(field.db_name)
    return column


def datetime_modifier(field: DatasetFieldSchema, column):
    if field.type == "string" and field.format == "date-time":
        return func.to_char(column, 'YYYY-MM-DD"T"HH24:MI:SS').label(field.db_name)
    return column
