import json
import os
from pathlib import Path
from typing import Callable
from urllib.parse import ParseResult, urlparse

import pytest
import sqlalchemy_utils
from geoalchemy2 import Geometry  # NoQA, needed to make postgis work
from requests_mock import Mocker
from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from schematools.importer.base import metadata
from schematools.types import DatasetSchema, ProfileSchema

HERE = Path(__file__).parent


# fixtures engine and dbengine provided by pytest-sqlalchemy,
# automatically discovered by pytest via setuptools entry-points.
# https://github.com/toirl/pytest-sqlalchemy/blob/master/pytest_sqlalchemy.py


@pytest.fixture()
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


@pytest.fixture(scope="session", autouse=True)
def db_schema(engine, sqlalchemy_keep_db):
    db_exists = sqlalchemy_utils.functions.database_exists(engine.url)
    if db_exists and not sqlalchemy_keep_db:
        raise RuntimeError("DB exists, remove it before proceeding")

    if not db_exists:
        sqlalchemy_utils.functions.create_database(engine.url)
        engine.execute("CREATE EXTENSION postgis")
    yield
    sqlalchemy_utils.functions.drop_database(engine.url)


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
def tconn(engine, local_metadata):
    """Will start a transaction on the connection. The connection will
    be rolled back after it leaves its scope.
    """

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            yield conn
        finally:
            transaction.rollback()


@pytest.fixture(scope="module")
def module_metadata(engine):
    """A module scoped metadata. This can be used to collect table structures
    during tests that are part of a particular module. At the module boundary, these tables
    are dropped. When Table models are constructed serveral times in these tests,
    the 'extend_existing' constructor arg. can be used, to avoid errors.
    Tables are just replaced in the same metadata object.
    """
    _meta = MetaData()
    yield _meta
    _meta.drop_all(bind=engine)


@pytest.fixture
def local_metadata(engine):
    """A function scoped metadata. Some tests really need to destroy and not update
    previous instances of SA Table objects.
    """
    _meta = MetaData()
    yield _meta
    _meta.drop_all(bind=engine)


@pytest.fixture(scope="session")
def salogger():
    """Enable logging for sqlalchemy, useful for debugging."""
    import logging

    logging.basicConfig()
    logger = logging.getLogger("sqlalchemy.engine")
    logger.setLevel(logging.DEBUG)


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
            f"{schema_url}afvalwegingen/afvalwegingen",
            content=fh.read(),
        )
    yield requests_mock


@pytest.fixture()
def schema_json(here) -> Callable[[str], dict]:
    def _json_fetcher(filename) -> dict:
        path = here / "files" / filename
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
