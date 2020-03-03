from sqlalchemy import inspect, Table, Column, String, Boolean, Integer, Float
from geoalchemy2 import Geometry


JSON_TYPE_TO_PG = {
    "string": String,
    "boolean": Boolean,
    "integer": Integer,
    "number": Float,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema": String,
    "https://geojson.org/schema/Geometry.json": Geometry(
        geometry_type="GEOMETRY", srid=28992
    ),
    "https://geojson.org/schema/Point.json": Geometry(
        geometry_type="POINT", srid=28992
    ),
}


def fetch_table_names(engine):
    """ Fetches all tablenames, to be used in other commands
    """
    insp = inspect(engine)
    return insp.get_table_names()


def fetch_pg_table(dataset_schema, table_name, metadata) -> Table:
    dataset_table = dataset_schema.get_table_by_id(table_name)
    table_key = f"{dataset_schema.id}_{table_name}"
    columns = [
        Column(field.name, JSON_TYPE_TO_PG[field.type])
        for field in dataset_table.fields
    ]
    return Table(table_key, metadata, *columns)


def create_rows(engine, metadata, dataset_schema, table_name, data):
    pg_table = fetch_pg_table(dataset_schema, table_name, metadata)
    engine.execute(pg_table.insert().values(), data)
