from __future__ import annotations

from schematools.introspect.geojson import geojson_to_table


def test_geojson_to_table():
    """Prove that GeoJSON can be parsed."""
    geojson = {
        "type": "FeatureCollection",
        "generator": "overpass-ide",
        "copyright": (
            "The data included in this document is from www.openstreetmap.org."
            " The data is made available under ODbL."
        ),
        "timestamp": "2019-11-15T10:27:02Z",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "@id": "relation/10191879",
                    "name": "gevaarlijke stoffen",
                    "route": "road",
                    "type": None,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [[4.9377626, 52.3868045]],
                },
                "id": "relation/10191879",
            },
            {
                "type": "Feature",
                "properties": {
                    "@id": "relation/10191879",
                    "name": "gevaarlijke stoffen",
                    "route": "road",
                    "type": 3,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [[4.9377626, 52.3868045]],
                },
                "id": "relation/10191879",
            },
        ],
    }

    result = geojson_to_table(iter(geojson["features"]), "unittest.geojson")
    assert result == [
        {
            "id": "unittest_relation",  # @id field was parsed as "relation"
            "type": "table",
            "version": "1.0.0",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": [],
                "display": "name",
                "properties": {
                    "schema": {
                        "$ref": (
                            "https://schemas.data.amsterdam.nl/schema@v1.3.0"
                            "#/definitions/schema"
                        )
                    },
                    "id": {"type": "string"},
                    "geometry": {"$ref": "https://geojson.org/schema/Point.json"},
                    "name": {"type": "string"},
                    "route": {"type": "string"},
                    "type": {"type": "number"},  # Introspected from 2nd record.
                },
            },
        }
    ]
