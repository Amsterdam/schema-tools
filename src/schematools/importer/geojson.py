from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any

from shapely.geometry import shape

from schematools.exceptions import ParserError
from schematools.types import DatasetTableSchema

from .base import BaseImporter, Record

ID_FORMAT = re.compile(r"^([a-z0-9_]+)[/.](\d+)$", re.I)


def read_geojson(file_name) -> Iterable[dict]:
    """Read the contents of a GeoJSON file into memory.
    This also performs basic checks whether the data is valid.
    """
    with open(file_name) as fh:
        geojson = json.load(fh)
        if (
            not isinstance(geojson, dict)
            or geojson.get("type") != "FeatureCollection"
            or not isinstance(geojson.get("features"), list)
        ):
            raise ParserError(f"{file_name} is not a valid GeoJSON file")

    for feature in geojson["features"]:
        feature_type = feature.get("type")
        if feature_type != "Feature":
            raise ParserError(f"Expected 'Feature' in {file_name}, not {feature_type}")

        yield feature


def split_id(id_value) -> tuple[str, str]:
    # When the ID format is name/identifier,
    # this detects that different feature types are part of the same file.
    match = ID_FORMAT.match(id_value)
    if match:
        return match.group(1), match.group(2)
    else:
        raise ValueError("Can't split ID value")


class GeoJSONImporter(BaseImporter):
    """Import an GeoJSON file into the database."""

    def parse_records(self, file_name, dataset_table: DatasetTableSchema, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        features = read_geojson(file_name)
        main_geometry = dataset_table.main_geometry_field

        relation_fields = [field for field in dataset_table.fields if field.relation is not None]
        for feature in features:
            record = Record(
                (self._clean_value(name, value) for name, value in feature["properties"].items()),
                source=feature,
            )
            for relation_field in relation_fields:
                if relation_field.db_name != relation_field.python_name:
                    record[relation_field.db_name] = record.pop(relation_field.python_name)

            wkt = shape(feature["geometry"]).wkt
            record[main_geometry.db_name] = f"SRID={main_geometry.srid};{wkt}"

            yield {dataset_table.id: [record]}

    def _clean_value(self, name: str, value: Any) -> tuple[str, Any]:
        if name[0] in "@$":
            name = name[1:]

        if name == "id":
            try:
                value = split_id(value)[1]
            except ValueError:
                pass

        return name, value
