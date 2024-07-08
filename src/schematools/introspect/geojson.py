"""Converting a GeoJSON input file to Amsterdam Schema"""

from __future__ import annotations

import re
from collections.abc import Iterable
from copy import deepcopy
from decimal import Decimal as D
from os.path import basename, splitext

from schematools.importer.geojson import read_geojson, split_id
from schematools.introspect.utils import DATASET_TMPL, TABLE_TMPL

ID_FORMAT = re.compile(r"^([a-z0-9_]+)[/.](\d+)$", re.I)


def introspect_geojson_files(dataset_id: str, files: list[str]) -> dict:
    """Generate Amsterdam Schema from GeoJSON files."""
    tables = []
    for file in files:
        tables.extend(introspect_geojson_file(file))

    aschema = deepcopy(DATASET_TMPL)
    aschema["id"] = dataset_id
    aschema["tables"] = tables
    return aschema


def introspect_geojson_file(file_name) -> list[dict]:
    """Convert a single GeoJSON file into a JSON Schema"""
    features = read_geojson(file_name)
    return geojson_to_table(features, file_name=file_name)


def geojson_to_table(geojson_features: Iterable[dict], file_name: str) -> list[dict]:
    """Read the GeoJSON contents, return the table with JSON Schema.

    :param filename: This is provided for error messages.
    """
    default_name = splitext(basename(file_name))[0]
    all_schema = {}
    all_properties = {}

    for feature in geojson_features:
        # Parse feature['id'], determine the datatype of this feature
        table_name, id_value = _parse_id(feature, default_name)

        # These values are filled by reference, linked in all_*
        schema = all_schema.setdefault(table_name, {})
        properties = all_properties.setdefault(table_name, {})

        # Fill the "properties" section of the table schema
        _fill_properties(feature, properties, id_value)

        # Auto-detect display field
        display_field = _get_display(feature)
        if display_field is not None:
            schema["display"] = display_field

        # When all types are determined, the loop can be exited.
        # If there were None values, the next record is examined.
        if all(properties.values()):
            break

    # Overlay all results into the amsterdam schem table format.
    result = []
    for name in all_schema:
        table = deepcopy(TABLE_TMPL)
        table["id"] = name if name == default_name else f"{default_name}_{name}"
        table["version"] = "1.0.0"  # Hardcoded, what else?
        table["schema"].update(all_schema[name])
        table["schema"]["properties"].update(all_properties[name])
        result.append(table)

    return result


def _parse_id(feature, default_name) -> tuple[str, str | None]:
    """Support datatype/PKVALUE as id value"""
    # Support optional "id" field at feature level
    try:
        id_value = feature["id"]
        return split_id(id_value)
    except (KeyError, ValueError):
        return default_name, None


def _fill_properties(feature: dict, properties: dict, id_value=None):
    """Fill the "properties" section of the table schema"""
    # Introspect "geometry" field at feature level
    if id_value is not None:
        properties["id"] = _build_geojson_field(id_value)

    geom_type = feature["geometry"]["type"]
    properties["geometry"] = {"$ref": f"https://geojson.org/schema/{geom_type}.json"}

    # Introspect remaining properties block
    for name, value in feature.get("properties", {}).items():
        if name[0] in "@$":
            name = name[1:]

        column = _build_geojson_field(value)
        if column is None and name in properties:
            # Don't override existing value with none
            continue

        properties[name] = column


def _get_display(feature: dict) -> str | None:
    """Auto-detect some fields as possible 'display' field"""
    properties = feature.get("properties", {})
    for name in ("name", "title"):
        if name in properties:
            return name

    return None


def _build_geojson_field(value):
    """Determine the table column based on a value."""
    if value is None:
        return None
    elif isinstance(value, bool):
        return {"type": "boolean"}
    elif isinstance(value, (int, float, D)):
        return {"type": "number"}
    else:
        return {"type": "string"}
