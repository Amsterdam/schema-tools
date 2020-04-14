DATASET_TMPL = {
    "type": "dataset",
    "id": None,
    "title": None,
    "status": "beschikbaar",
    "description": None,
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
