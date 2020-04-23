import json
import re
from typing import Any, Iterable, Tuple

from shapely.geometry import shape

from ..exceptions import ParserError
from .base import BaseImporter

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


def split_id(id_value) -> Tuple[str, str]:
    # When the ID format is name/identifier,
    # this detects that different feature types are part of the same file.
    match = ID_FORMAT.match(id_value)
    if match:
        return match.group(1), match.group(2)
    else:
        raise ValueError("Can't split ID value")


class GeoJSONImporter(BaseImporter):
    """Import an GeoJSON file into the database."""

    def parse_records(self, file_name, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        features = read_geojson(file_name)
        for feature in features:
            wkt = shape(feature["geometry"]).wkt
            record = dict(
                self._clean_value(name, value)
                for name, value in feature["properties"].items()
            )
            record["geometry"] = f"SRID={self.srid};{wkt}"
            yield record

    def _clean_value(self, name: str, value: Any) -> Tuple[str, Any]:
        if name[0] in "@$":
            name = name[1:]

        if name == "id":
            try:
                value = split_id(value)[1]
            except ValueError:
                pass

        return name, value
