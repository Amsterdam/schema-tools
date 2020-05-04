import os
from pathlib import Path
from urllib.parse import urlparse, ParseResult

import pytest
from requests_mock import Mocker
from sqlalchemy.orm import Session

from schematools import models

HERE = Path(__file__).parent


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
    models.Base.metadata.create_all(bind=engine)
    try:
        yield dbsession
    finally:
        dbsession.close()

        # Drop all test tables after the tests completed
        if not sqlalchemy_keep_db:
            models.Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def schemas_mock(requests_mock: Mocker, schema_url):
    """Mock the requests to import schemas.

    This allows to run "schema import schema afvalwegingen".
    """
    # `requests_mock` is a fixture from the requests_mock package
    afvalwegingen_json = HERE / "files" / "afvalwegingen.json"
    requests_mock.get(
        schema_url,
        json=[
            {
                "name": "afvalwegingen",
                "type": "directory",
                "mtime": "Wed, 08 Apr 2020 09:35:31 GMT",
            }
        ],
    )
    requests_mock.get(
        f"{schema_url}afvalwegingen/",
        json=[
            {
                "name": "afvalwegingen",
                "type": "file",
                "mtime": "Wed, 08 Apr 2020 09:35:31 GMT",
                "size": 11122,
            }
        ],
    )
    with open(afvalwegingen_json, "rb") as fh:
        requests_mock.get(
            f"{schema_url}afvalwegingen/afvalwegingen",
            content=fh.read(),
        )
    yield requests_mock
