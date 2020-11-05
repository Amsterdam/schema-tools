import json
from pathlib import Path
import pytest
from typing import Callable
from schematools.types import DatasetSchema

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
