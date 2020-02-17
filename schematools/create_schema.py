import copy
import json

from sqlalchemy import inspect
from geoalchemy2.types import Geometry
from sqlalchemy.types import DATE, VARCHAR, INTEGER, BOOLEAN, TEXT, NUMERIC, SMALLINT
from sqlalchemy.dialects.postgresql.base import TIMESTAMP
from sqlalchemy.dialects.postgresql.array import ARRAY

DATASET_TMPL = {
    "type": "dataset",
    "id": None,
    "title": None,
    "status": "beschikbaar",
    "version": "0.0.1",
    "crs": "EPSG:28992",
    "tables": [],
}


# The display field will be hard-coded as 'id', because we cannot know this value
# by purely inspecting the postgresql db.
TABLE_TMPL = {
    "id": None,
    "type": "table",
    "schema": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": False,
        "required": [],
        "display": "id",
        "properties": {
            "schema": {
                "$ref": "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema"
            },
        },
    },
}

# the Geometry field has a property geometry_type, could be mapped to more
# specific types in geojson.org

DB_TO_ASCHEMA_TYPE = {
    DATE: {"type": "string", "format": "date"},
    TIMESTAMP: {"type": "string", "format": "date-time"},
    VARCHAR: {"type": "string"},
    INTEGER: {"type": "integer"},
    SMALLINT: {"type": "integer"},
    NUMERIC: {"type": "number"},
    BOOLEAN: {"type": "integer"},
    TEXT: {"type": "string"},
    ARRAY: {"type": "string"},
    Geometry: {"$ref": "https://geojson.org/schema/Geometry.json"},
}


def fix_name(field_name, field_value=None):
    if field_value is None or "relation" in field_value:
        ret = field_name.replace("_id", "").replace("_", " ")
    else:
        ret = field_name.replace("_", " ")
    return ret


def fetch_schema_for(engine, dataset_id, tablenames, prefix=None):
    insp = inspect(engine)
    tables = []

    for full_table_name in tablenames:
        table_name = full_table_name
        if prefix is not None:
            table_name = full_table_name[len(prefix) :]
        columns = {}
        relations = {}
        required_field_names = ["schema"]
        fks = insp.get_foreign_keys(full_table_name)
        for fk in fks:
            if len(fk["constrained_columns"]) > 1:
                raise Exception("More than one fk col")
            constrained_column = fk["constrained_columns"][0]
            if not constrained_column.startswith("_"):
                relations[constrained_column] = fk["referred_table"]
        pk_info = insp.get_pk_constraint(full_table_name)
        if len(pk_info["constrained_columns"]) > 1:
            raise Exception("multicol pk")
        if len(pk_info["constrained_columns"]) == 0:
            raise Exception("no pk")
        # pk = pk_info["constrained_columns"][0]  # ASchema assumes 'id' for the pk
        for col in insp.get_columns(full_table_name):  # name, type, nullable
            col_name = col["name"]
            if col_name.startswith("_"):
                continue
            col_type = col["type"].__class__
            if not col["nullable"]:
                required_field_names.append(col_name)
            columns[col_name] = DB_TO_ASCHEMA_TYPE[col_type].copy()
            # XXX Add 'title' and 'description' to the column

        for field_name, referred_table in relations.items():
            columns[field_name].update({"relation": referred_table.replace("_", ":")})
        table = copy.deepcopy(TABLE_TMPL)
        table["id"] = table_name
        table["schema"]["required"] = [
            fix_name(fn, fv)
            for fn, fv in map(lambda n: (n, columns.get(n, None)), required_field_names)
        ]
        table["schema"]["properties"].update(
            {fix_name(fn, fv): fv for fn, fv in columns.items()}
        )
        tables.append(table)

    dataset = copy.deepcopy(DATASET_TMPL)
    dataset["id"] = dataset_id
    dataset["title"] = dataset_id
    dataset["tables"] = tables

    return json.dumps(dataset)
