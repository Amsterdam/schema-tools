import copy

from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from string_utils import snake_case_to_camel

from .models import Dataset, Table

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
                "$ref": "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema"
            },
        },
    },
}

# the Geometry field has a property geometry_type, could be mapped to more
# specific types in geojson.org

DB_TO_ASCHEMA_TYPE = {
    "DATE": {"type": "string", "format": "date"},
    "TIMESTAMP": {"type": "string", "format": "date-time"},
    "VARCHAR": {"type": "string"},
    "INTEGER": {"type": "integer"},
    "SMALLINT": {"type": "integer"},
    "NUMERIC": {"type": "number"},
    "BOOLEAN": {"type": "boolean"},
    "TEXT": {"type": "string"},
    "ARRAY": {"type": "array"},
    "GEOMETRY": {"$ref": "https://geojson.org/schema/Geometry.json"},
    "POLYGON": {"$ref": "https://geojson.org/schema/Polygon.json"},
    "MULTIPOLYGON": {"$ref": "https://geojson.org/schema/MultiPolygon.json"},
    "POINT": {"$ref": "https://geojson.org/schema/Point.json"},
    "MULTIPOINT": {"$ref": "https://geojson.org/schema/MultiPoint.json"},
    "LINESTRING": {"$ref": "https://geojson.org/schema/LineString.json"},
    "MULTILINESTRING": {"$ref": "https://geojson.org/schema/MultiLineString.json"},
    "GEOMETRYCOLLECTION": {
        "$ref": "https://geojson.org/schema/GeometryCollection.json"
    },
}


def fix_name(field_name, field_value=None):
    if field_value is None or "relation" in field_value:
        ret = field_name.replace("_id", "").replace("_", " ")
    else:
        ret = field_name.replace("_", " ")
    return ret


def fetch_schema_for_db(engine, dataset_id, tablenames, prefix=None):
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
            col_type = col["type"].__class__.__name__
            if col_type == "Geometry":
                col_type = col["type"].geometry_type
            if not col["nullable"]:
                required_field_names.append(col_name)
            aschema_type = DB_TO_ASCHEMA_TYPE[col_type].copy()
            columns[col_name] = aschema_type
            if col_type == "ARRAY":
                item_type = col["type"].item_type.__class__.__name__
                aschema_type["items"] = DB_TO_ASCHEMA_TYPE[item_type]
            # XXX Add 'title' and 'description' to the column

        for field_name, referred_table in relations.items():
            columns[field_name].update(
                {"relation": referred_table.replace("_", ":", 1)}
            )

        # Generate table section
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

    # Generate main section
    dataset = copy.deepcopy(DATASET_TMPL)
    dataset["id"] = dataset_id
    dataset["title"] = dataset_id
    dataset["tables"] = tables
    return dataset


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


def fetch_schema_from_relational_schema(engine, dataset_id):
    session = sessionmaker(bind=engine)()
    dataset = (
        session.query(Dataset)
        .join(Dataset.tables)
        .join(Table.fields)
        .filter(Dataset.id == dataset_id)
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
