from collections import defaultdict

from dateutil.parser import parse as dtparse
from geoalchemy2 import Geometry
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, inspect
from sqlalchemy.orm import sessionmaker
from string_utils import camel_case_to_snake

from . import models

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


def create_meta_tables(engine):
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)


def transformer_factory(model):

    transforms = defaultdict(lambda: lambda x: x)
    transforms[DateTime] = lambda x: x and dtparse(x) or None

    transform_lookup = {
        col.name: transforms[col.type.__class__] for col in model.__table__.c
    }

    def _transformer(content):
        return {k: transform_lookup[k](v) for k, v in content.items()}

    return _transformer


def create_meta_table_data(engine, dataset_schema):
    session = sessionmaker(bind=engine)()
    ds_content = {
        camel_case_to_snake(k): v for k, v in dataset_schema.items() if k != "tables"
    }
    ds_content["contact_point"] = str(ds_content["contact_point"])
    ds_transformer = transformer_factory(models.Dataset)
    dataset = models.Dataset(**ds_transformer(ds_content))
    session.add(dataset)

    for table_data in dataset_schema["tables"]:
        table_content = {
            camel_case_to_snake(k): v for k, v in table_data.items() if k != "schema"
        }

        table = models.Table(
            **{
                **table_content,
                **{f: table_data["schema"][f] for f in ("required", "display")},
            }
        )
        table.dataset_id = dataset.id
        session.add(table)

        for field_name, field_value in table_data["schema"]["properties"].items():
            field_content = {
                k.replace("$", ""): v
                for k, v in field_value.items()  # if not k.startswith("$")
            }
            field_content["name"] = field_name
            field = models.Field(**field_content)

            field.table_id = table.id
            field.dataset_id = dataset.id
            session.add(field)

    session.commit()
