import os
from pathlib import Path
from urllib.parse import ParseResult, urlparse

import pytest
import sqlalchemy_utils
from geoalchemy2 import Geometry  # NoQA, needed to make postgis work
from requests_mock import Mocker
from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from schematools.importer.base import metadata

HERE = Path(__file__).parent

pytest_plugins = ["tests.fixtures"]

# fixtures engine and dbengine provided by pytest-sqlalchemy,
# automatically discovered by pytest via setuptools entry-points.
# https://github.com/toirl/pytest-sqlalchemy/blob/master/pytest_sqlalchemy.py


@pytest.fixture(scope="session")
def db_url() -> str:
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
