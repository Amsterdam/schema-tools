"""Datbase storage of metadata from imported Amsterdam schema files."""
from collections import defaultdict

from dateutil.parser import parse as dtparse
from geoalchemy2 import Geometry
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, inspect
from sqlalchemy.orm import sessionmaker
from string_utils import camel_case_to_snake, snake_case_to_camel

from schematools import models
from schematools.utils import ParserError

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
        try:
            return {k: transform_lookup[k](v) for k, v in content.items()}
        except ValueError as e:
            raise ParserError(f"Failed to parse row: {e}, at {content!r}") from e

    return _transformer


def create_meta_table_data(engine, dataset_schema):
    session = sessionmaker(bind=engine)()
    ds_content = {
        camel_case_to_snake(k): v for k, v in dataset_schema.items() if k != "tables"
    }
    ds_content["contact_point"] = str(ds_content.get("contact_point", ""))
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
                **{f: table_data["schema"].get(f) for f in ("required", "display")},
            }
        )
        table.dataset_id = dataset.id
        session.add(table)

        for field_name, field_value in table_data["schema"]["properties"].items():
            field_content = {
                k.replace("$", ""): v
                for k, v in field_value.items()
                if not k in {"$comment"}
            }
            field_content["name"] = field_name
            try:
                field = models.Field(**field_content)
            except TypeError as e:
                raise NotImplementedError(
                    f'Import failed: at "{field_name}": {field_value!r}:\n{e}'
                ) from e

            field.table_id = table.id
            field.dataset_id = dataset.id
            session.add(field)

    session.commit()


def fetch_schema_from_relational_schema(engine, dataset_id) -> dict:
    """Restore the schema based on the stored metadata"""
    session = sessionmaker(bind=engine)()
    dataset = (
        session.query(models.Dataset)
        .join(models.Dataset.tables)
        .join(models.Table.fields)
        .filter(models.Dataset.id == dataset_id)
        .first()
    )
    if not dataset:
        raise ValueError(f"Dataset {dataset_id} not found.")

    aschema = _serialize(dataset)
    aschema["tables"] = [_serialize(t) for t in aschema["tables"]]
    for table_dict in aschema["tables"]:
        del table_dict["dataset"]
        del table_dict["datasetId"]
        table_dict["schema"] = {f: table_dict.get(f) for f in ("required", "display")}
        table_dict["schema"]["$schema"] = "http://json-schema.org/draft-07/schema#"
        table_dict["schema"]["type"] = "object"
        table_dict["schema"]["additionalProperties"] = False
        table_dict.pop("required", "")
        table_dict.pop("display", "")
        properties = [_serialize(f, camelize=False) for f in table_dict["fields"]]
        del table_dict["fields"]
        for prop in properties:
            del prop["table"]
            del prop["dataset_id"]
            del prop["table_id"]
            ref = prop.pop("ref", None)
            if ref is not None:
                prop["$ref"] = ref
        table_dict["schema"]["properties"] = list(_extract_names(properties))
    return aschema


def _serialize(obj, camelize=True):
    results = {}
    for attr in inspect(obj).attrs:
        value = attr.value
        key = attr.key
        if camelize:
            key = snake_case_to_camel(key, upper_case_first=False)
        if value is None:
            continue
        if hasattr(value, "isoformat"):
            value = attr.value.isoformat()
        results[key] = value
    return results


def _extract_names(properties):
    for prop in properties:
        name = prop.pop("name").replace("_", " ")
        yield {name: prop}
