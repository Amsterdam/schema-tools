import re

import json_encoder.json
import pytest

from schematools import ckan
from schematools.types import DatasetSchema


@pytest.mark.parametrize(
    "name",
    [
        "afval",
        "afvalwegingen",
        "brk",
        "brp",
        "composite_key",
        "ggwgebieden",
        "hr",
        "meetbouten",
        "meldingen",
        "parkeervakken",
        "stadsdelen",
    ],
)
def test_convert(here, name):
    filename = here / "files" / (name + ".json")
    with open(filename) as f:
        schema = DatasetSchema.from_dict(json_encoder.json.load(f))

    data = ckan.from_dataset(schema)

    for key in ["identifier", "title"]:
        assert data.get(key) and re.match(r"^[a-z0-9_-]", data[key])
    for key in ["language", "theme"]:
        assert isinstance(data.get(key), list) and data[key]
    assert data["identifier"]
    assert data["identifier"] == data["url"]