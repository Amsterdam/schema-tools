"""Datbase storage of metadata from imported Amsterdam schema files."""
import json

from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker

from schematools import models
from schematools.utils import toCamelCase


def fetch_table_names(engine):
    """Fetches all tablenames, to be used in other commands."""
    insp = inspect(engine)
    return insp.get_table_names()


def fetch_schema_from_relational_schema(engine, dataset_id) -> dict:
    """Restore the schema based on the stored metadata."""
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
    contact_point = aschema["contactPoint"]
    aschema["contactPoint"] = json.loads(contact_point)
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
            elif "geojson" in prop["type"]:
                prop["$ref"] = prop["type"]
                del prop["type"]
        schema_properties = _extract_names(properties)
        schema_version = table_dict.pop("schemaVersion")
        schema_properties["schema"] = {
            "$ref": (
                f"https://schemas.data.amsterdam.nl/schema@{schema_version}#/definitions/schema"
            )
        }
        table_dict["schema"]["properties"] = schema_properties
    return aschema


def _serialize(obj, camelize=True):
    results = {}
    for attr in inspect(obj).attrs:
        value = attr.value
        key = attr.key
        if camelize:
            key = toCamelCase(key, upper_case_first=False)
        if value is None:
            continue
        if hasattr(value, "isoformat"):
            value = attr.value.isoformat()
        results[key] = value
    return results


def _extract_names(properties):
    return {toCamelCase(prop.pop("name")): prop for prop in properties}
