from schematools.types import DatasetTableSchema

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Float,
    Integer,
    String,
    Date,
    Time,
    DateTime,
)

FORMAT_MODELS_LOOKUP = {
    "date": Date,
    "time": Time,
    "date-time": DateTime,
    "uri": String,
    "email": String,
}

JSON_TYPE_TO_PG = {
    "string": String,
    "object": String,
    "boolean": Boolean,
    "integer": Integer,
    "number": Float,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema": String,
    "https://geojson.org/schema/Geometry.json": Geometry(
        geometry_type="GEOMETRY", srid=28992
    ),
    "https://geojson.org/schema/Point.json": Geometry(
        geometry_type="POINT", srid=28992
    ),
    "https://geojson.org/schema/Polygon.json": Geometry(
        geometry_type="POLYGON", srid=28992
    ),
    "https://geojson.org/schema/MultiPolygon.json": Geometry(
        geometry_type="MULTIPOLYGON", srid=28992
    ),
    "https://geojson.org/schema/MultiPoint.json": Geometry(
        geometry_type="MULTIPOINT", srid=28992
    ),
    "https://geojson.org/schema/LineString.json": Geometry(
        geometry_type="LINESTRING", srid=28992
    ),
    "https://geojson.org/schema/MultiLineString.json": Geometry(
        geometry_type="MULTILINESTRING", srid=28992
    ),
}


def fetch_col_type(field):
    col_type = JSON_TYPE_TO_PG[field.type]
    # XXX no walrus until we can go to python 3.8 (airflow needs 3.7)
    # if (field_format := field.format) is not None:
    field_format = field.format
    if field_format is not None:
        return FORMAT_MODELS_LOOKUP[field_format]
    return col_type


def get_table_name(dataset_table: DatasetTableSchema) -> str:
    """Generate the database identifier for the table."""
    schema = dataset_table._parent_schema
    return f"{schema.id}_{dataset_table.id}".replace("-", "_")
