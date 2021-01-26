import copy

from sqlalchemy import inspect

from ..utils import toCamelCase
from .utils import DATASET_TMPL, TABLE_TMPL

# the Geometry field has a property geometry_type, could be mapped to more
# specific types in geojson.org

DB_TO_ASCHEMA_TYPE = {
    "DATE": {"type": "string", "format": "date"},
    "TIME": {"type": "string", "format": "time"},
    "TIMESTAMP": {"type": "string", "format": "date-time"},
    "VARCHAR": {"type": "string"},
    "INTEGER": {"type": "integer"},
    "BIGINT": {"type": "integer"},
    "SMALLINT": {"type": "integer"},
    "NUMERIC": {"type": "number"},
    "DOUBLE_PRECISION": {"type": "number"},
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
    "GEOMETRYCOLLECTION": {"$ref": "https://geojson.org/schema/GeometryCollection.json"},
}


def fix_name(field_name, field_value=None):
    ret = field_name
    if field_value is None or "relation" in field_value:
        ret = field_name.replace("_id", "")
    return toCamelCase(ret)


def introspect_db_schema(engine, dataset_id, tablenames, db_schema=None, prefix=None):
    """Generate an amsterdam schema file based on an existing database."""
    insp = inspect(engine)
    tables = []

    for full_table_name in tablenames:
        table_name = full_table_name
        if prefix is not None:
            table_name = full_table_name[len(prefix) :]
        columns = {}
        relations = {}
        required_field_names = ["schema"]
        fks = insp.get_foreign_keys(full_table_name, schema=db_schema)
        for fk in fks:
            if len(fk["constrained_columns"]) > 1:
                raise Exception("More than one fk col")
            constrained_column = fk["constrained_columns"][0]
            if not constrained_column.startswith("_"):
                relations[constrained_column] = fk["referred_table"]
        pk_info = insp.get_pk_constraint(full_table_name, schema=db_schema)
        if len(pk_info["constrained_columns"]) > 1:
            raise Exception("multicol pk")
        if len(pk_info["constrained_columns"]) == 0:
            raise Exception("no pk")
        # pk = pk_info["constrained_columns"][0]  # ASchema assumes 'id' for the pk
        for col in insp.get_columns(full_table_name, schema=db_schema):  # name, type, nullable
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
            columns[field_name].update({"relation": referred_table.replace("_", ":", 1)})

        # Generate table section
        table = copy.deepcopy(TABLE_TMPL)
        table["id"] = table_name
        table["schema"]["required"] = [
            fix_name(fn, fv)
            for fn, fv in map(lambda n: (n, columns.get(n, None)), required_field_names)
        ]
        table["schema"]["properties"].update({fix_name(fn, fv): fv for fn, fv in columns.items()})
        tables.append(table)

    # Generate main section
    dataset = copy.deepcopy(DATASET_TMPL)
    dataset["id"] = dataset_id
    dataset["title"] = dataset_id
    dataset["tables"] = tables
    return dataset
