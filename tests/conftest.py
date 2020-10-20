import os
import json
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse, ParseResult

import pytest
from requests_mock import Mocker
from sqlalchemy.orm import Session

from schematools.types import DatasetSchema
from schematools.importer.base import metadata

HERE = Path(__file__).parent


@pytest.fixture(scope="session")
def here():
    return HERE


@pytest.fixture(scope="session")
def db_url():
    """Get the DATABASE_URL, prepend test_ to it."""
    url = os.environ.get("DATABASE_URL", "postgresql://localhost/schematools")

    parts = urlparse(url)
    dbname = parts.path[1:]

    # ParseResult is a namedtuple so need to cast to an editable type
    parts: dict = dict(parts._asdict())
    parts["path"] = f"test_{dbname}"
    return ParseResult(**parts).geturl()


@pytest.fixture(scope="session")
def sqlalchemy_connect_url(request, db_url):
    """Override pytest-sqlalchemy fixture to use default db_url instead."""
    return request.config.getoption("--sqlalchemy-connect-url") or db_url


@pytest.fixture()
def schema_url():
    return os.environ.get("SCHEMA_URL", "https://schemas.data.amsterdam.nl/datasets/")


@pytest.fixture(scope="function")
def dbsession(engine, dbsession, sqlalchemy_keep_db) -> Session:
    """Override the 'dbsession' to create filled database tables."""
    try:
        yield dbsession
    finally:
        # Drop all test tables after the tests completed
        if not sqlalchemy_keep_db:
            metadata.drop_all(bind=engine)
        metadata.clear()
        dbsession.close()




@pytest.fixture()
def schemas_mock(requests_mock: Mocker, schema_url):
    """Mock the requests to import schemas.

    This allows to run "schema import schema afvalwegingen".
    """
    # `requests_mock` is a fixture from the requests_mock package
    afvalwegingen_json = HERE / "files" / "afvalwegingen.json"
    requests_mock.get(
        f"{schema_url}index.json", json={"afvalwegingen": "afvalwegingen/afvalwegingen"}
    )
    with open(afvalwegingen_json, "rb") as fh:
        requests_mock.get(
            f"{schema_url}afvalwegingen/afvalwegingen", content=fh.read(),
        )
    yield requests_mock


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

