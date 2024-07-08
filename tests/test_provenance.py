from __future__ import annotations

from schematools.provenance.create import ProvenanceIteration


def test_provenance_result():
    """Prove that ``provenance`` elements are destillerated from input argument"""

    test_data = {
        "type": "dataset",
        "id": "winkelgebieden",
        "title": "winkelgebieden",
        "status": "beschikbaar",
        "description": "Winkelgebieden",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "tables": [
            {
                "id": "winkelgebieden",
                "type": "table",
                "version": "1.0.0",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema", "id"],
                    "display": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "integer"},
                        "wkb geometry": {"$ref": "https://geojson.org/schema/Geometry.json"},
                        "translated_colname": {
                            "type": "string",
                            "provenance": "source_colname",
                        },
                        "translated_colname2": {
                            "type": "string",
                            "provenance": "source_colname2",
                        },
                        "translated_colname3": {
                            "type": "string",
                            "provenance": "source_colname3",
                        },
                        "translated_colname4": {
                            "type": "string",
                            "provenance": "source_colname4",
                        },
                    },
                },
            }
        ],
    }

    def count_num_of_levels(data):
        """Always three levels: dataset, table and property, even if empty"""
        if not data:
            return 1

        else:

            if isinstance(data, dict):
                return 1 + max(count_num_of_levels(data[item]) for item in data)
            elif isinstance(data, list):
                return 0 + max(count_num_of_levels(item) for item in data)
            else:
                return 0

    result = ProvenanceIteration(test_data)
    result = result.final_dic
    result = count_num_of_levels(result)
    assert result == 3  # always three levels: dataset, table and property


print(test_provenance_result())
