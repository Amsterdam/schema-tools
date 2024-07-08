from __future__ import annotations

DATASET_TMPL = {
    "type": "dataset",
    "id": None,
    "title": None,
    "status": "beschikbaar",
    "description": None,
    "crs": "EPSG:28992",
    "auth": "OPENBAAR",
    "authorizationGrantor": "n.v.t.",
    "owner": "Gemeente Amsterdam",
    "creator": "bronhouder onbekend",
    "publisher": "Datateam xyz",
    "tables": [],
}

# The display field will be hard-coded as 'id', because we cannot know this value
# by purely inspecting the postgresql db.
TABLE_TMPL = {
    "id": None,
    "type": "table",
    "version": "0.0.1",
    "schema": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": False,
        "required": [],
        "display": "id",
        "properties": {
            "schema": {
                "$ref": "https://schemas.data.amsterdam.nl/schema@v1.3.0#/definitions/schema"
            },
        },
    },
}
