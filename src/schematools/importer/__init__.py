import numbers
from decimal import Decimal

from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, Numeric, String, Time
from sqlalchemy.types import ARRAY

from schematools.types import DatasetTableSchema

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
    "integer": BigInteger,
    "integer/autoincrement": BigInteger,
    "number": Float,
    "array": ARRAY(String),
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema": String,
    "https://geojson.org/schema/Geometry.json": Geometry(geometry_type="GEOMETRY", srid=28992),
    "https://geojson.org/schema/Point.json": Geometry(geometry_type="POINT", srid=28992),
    "https://geojson.org/schema/Polygon.json": Geometry(geometry_type="POLYGON", srid=28992),
    "https://geojson.org/schema/MultiPolygon.json": Geometry(
        geometry_type="MULTIPOLYGON", srid=28992
    ),
    "https://geojson.org/schema/MultiPoint.json": Geometry(geometry_type="MULTIPOINT", srid=28992),
    "https://geojson.org/schema/LineString.json": Geometry(geometry_type="LINESTRING", srid=28992),
    "https://geojson.org/schema/MultiLineString.json": Geometry(
        geometry_type="MULTILINESTRING", srid=28992
    ),
}


def numeric_datatype_scale(scale_=None):
    """detect scale from decimal for database datatype scale definition"""
    if (
        isinstance(scale_, numbers.Number)
        and str(scale_).count("1") == 1
        and str(scale_).endswith("1")
    ):
        # TODO: make it possible to set percision too
        # now it defaults to max of 12
        get_scale = Decimal(str(scale_)).as_tuple().exponent
        if get_scale < 0:
            get_scale = get_scale * -1
        return Numeric(precision=12, scale=get_scale)
    else:
        return Numeric


def fetch_col_type(field):
    col_type = JSON_TYPE_TO_PG[field.type]
    if (field_format := field.format) is not None:
        return FORMAT_MODELS_LOOKUP[field_format]
    # TODO: format takes precedence over multipleof
    # if there is an use case that both can apply for a field definition
    # then logic must be changed
    field_multiple = field.multipleof
    if field_multiple is not None:
        return numeric_datatype_scale(scale_=field_multiple)
    return col_type


def get_table_name(dataset_table: DatasetTableSchema) -> str:
    """Generate the database identifier for the table."""
    schema = dataset_table._parent_schema
    return f"{schema.id}_{dataset_table.id}".replace("-", "_")
