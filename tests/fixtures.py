import json
from pathlib import Path
from typing import Callable

import pytest

from schematools.types import DatasetSchema, ProfileSchema

HERE = Path(__file__).parent


@pytest.fixture(scope="session")
def here():
    return HERE


@pytest.fixture()
def schema_json() -> Callable[[str], dict]:
    def _json_fetcher(filename) -> dict:
        path = HERE / "files" / filename
        return json.loads(path.read_text())

    return _json_fetcher


@pytest.fixture()
def afval_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("afval.json"))


@pytest.fixture()
def meetbouten_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("meetbouten.json"))


@pytest.fixture()
def parkeervakken_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("parkeervakken.json"))


@pytest.fixture()
def gebieden_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("gebieden.json"))


@pytest.fixture()
def bouwblokken_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("bouwblokken.json"))


@pytest.fixture()
def gebieden_schema_auth(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("gebieden_auth.json"))


@pytest.fixture()
def gebieden_schema_auth_list(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("gebieden_auth_list.json"))


@pytest.fixture()
def ggwgebieden_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("ggwgebieden.json"))


@pytest.fixture()
def stadsdelen_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("stadsdelen.json"))


@pytest.fixture()
def verblijfsobjecten_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("verblijfsobjecten.json"))


@pytest.fixture()
def kadastraleobjecten_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("kadastraleobjecten.json"))


@pytest.fixture()
def meldingen_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("meldingen.json"))


@pytest.fixture()
def woonplaatsen_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("woonplaatsen.json"))


@pytest.fixture()
def woningbouwplannen_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("woningbouwplannen.json"))


@pytest.fixture()
def brp_r_profile_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    path = here / "files/profiles/BRP_R.json"
    return ProfileSchema.from_file(path)


@pytest.fixture()
def brk_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("brk.json"))


@pytest.fixture()
def hr_schema(schema_json) -> DatasetSchema:
    return DatasetSchema.from_dict(schema_json("hr.json"))
