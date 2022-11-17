from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager
from urllib.parse import ParseResult, urlparse

import pytest
import sqlalchemy_utils
from more_ds.network.url import URL
from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.sql.ddl import DropTable

from schematools.importer.base import metadata
from schematools.types import DatasetSchema, Json, ProfileSchema
from schematools.utils import dataset_schema_from_path

HERE = Path(__file__).parent


try:
    # Will raise ImproperlyConfigured if env. vars not set.
    import django  # noqa: F401
except Exception:
    collect_ignore_glob = ["django"]


# fixtures engine and dbengine provided by pytest-sqlalchemy,
# automatically discovered by pytest via setuptools entry-points.
# https://github.com/toirl/pytest-sqlalchemy/blob/master/pytest_sqlalchemy.py


@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    # A simple `engine.drop_all` is no sufficient anymore now that we also create views.
    # We need a `CASCADE` for these views to be correctly dropped as well.
    return compiler.visit_drop_table(element) + " CASCADE"


@pytest.fixture
def here() -> Path:
    return HERE


@pytest.fixture(scope="session")
def db_url():
    """Get the DATABASE_URL, prepend test_ to it."""
    url = os.environ.get("DATABASE_URL")
    if url is None:
        pytest.skip("DATABASE_URL not set")

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


@pytest.fixture(scope="session")
def db_schema(engine, sqlalchemy_keep_db):
    db_exists = sqlalchemy_utils.functions.database_exists(engine.url)
    if db_exists and not sqlalchemy_keep_db:
        raise RuntimeError("DB exists, remove it before proceeding")

    if not db_exists:
        sqlalchemy_utils.functions.create_database(engine.url)
        engine.execute("CREATE EXTENSION postgis")
    yield
    sqlalchemy_utils.functions.drop_database(engine.url)


@pytest.fixture
def schema_url():
    return URL(os.environ.get("SCHEMA_URL", "https://schemas.data.amsterdam.nl/datasets/"))


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


@pytest.fixture
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


@pytest.fixture
def salogger():
    """Enable logging for sqlalchemy, useful for debugging."""
    import logging

    logging.basicConfig()
    logger = logging.getLogger("sqlalchemy.engine")
    logger.setLevel(logging.DEBUG)
    yield
    logger.setLevel(logging.WARNING)


class DummyResponse:
    """Class that mimicks requests.Response."""

    def __init__(self, content: Json):
        self.content = content

    def json(self) -> Json:
        return self.content

    def raise_for_status(self) -> None:
        pass


class DummySession:
    """Class that mimicks requests.Session."""

    def __init__(self, maker: DummySessionMaker):
        self.maker = maker

    def get(self, url: URL) -> DummyResponse:
        content = self.maker.fetch_content_for(url)
        return DummyResponse(content)


class DummySessionMaker:
    """Helper fixture that can produce a contextmanager.

    This helper can be configured with several routes.
    These routes will be mocked with a predefined json response.
    This helper class is a callable and return a contextmanager
    that mimicks `requests.Session`.
    """

    def __init__(self):
        self.routes: dict[URL, Json] = {}

    def add_route(self, path: URL, content: Json) -> None:
        self.routes[path] = content

    def fetch_content_for(self, url: URL) -> Json:
        return self.routes[url]

    def __call__(self) -> ContextManager[None]:
        @contextmanager
        def dummy_session() -> ContextManager[None]:
            yield DummySession(self)

        return dummy_session()


@pytest.fixture
def schemas_mock(schema_url: URL, monkeypatch: Any) -> DummySessionMaker:
    """Mock the requests to import schemas.

    This allows to run "schema import schema afvalwegingen".
    """

    from schematools.utils import requests

    dummy_session_maker = DummySessionMaker()

    AFVALWEGINGEN_JSON = HERE / "files" / "afvalwegingen_sep_table.json"
    CLUSTERS_JSON = HERE / "files" / "afvalwegingen_clusters" / "v1.0.0.json"
    VERBLIJFSOBJECTEN_JSON = HERE / "files" / "verblijfsobjecten.json"

    monkeypatch.setattr(requests, "Session", dummy_session_maker)

    dummy_session_maker.add_route(
        schema_url / "index.json", {"afvalwegingen": "afvalwegingen", "bag": "bag"}
    )

    with open(AFVALWEGINGEN_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "afvalwegingen/dataset",
            content=json.load(fh),
        )

    with open(VERBLIJFSOBJECTEN_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "bag/dataset",
            content=json.load(fh),
        )

    with open(CLUSTERS_JSON, "rb") as fh:
        dummy_session_maker.add_route(
            schema_url / "afvalwegingen/afvalwegingen_clusters" / "v1.0.0",
            content=json.load(fh),
        )
    yield dummy_session_maker


@pytest.fixture
def afval_schema_json(here: Path) -> Json:
    with open(here / "files/afval.json") as f:
        return json.load(f)


@pytest.fixture
def afval_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/afval.json")


@pytest.fixture
def afvalwegingen_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/afvalwegingen.json")


@pytest.fixture
def kadastraleobjecten_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/kadastraleobjecten.json")


@pytest.fixture
def meetbouten_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/meetbouten.json")


@pytest.fixture
def parkeervakken_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/parkeervakken.json")


@pytest.fixture
def gebieden_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/gebieden.json")


@pytest.fixture
def bouwblokken_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/bouwblokken.json")


@pytest.fixture
def gebieden_schema_auth(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/gebieden_auth.json")


@pytest.fixture
def gebieden_schema_auth_list(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/gebieden_auth_list.json")


@pytest.fixture
def ggwgebieden_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/ggwgebieden.json")


@pytest.fixture
def stadsdelen_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/stadsdelen.json")


@pytest.fixture
def verblijfsobjecten_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/verblijfsobjecten.json")


@pytest.fixture
def meldingen_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/meldingen.json")


@pytest.fixture
def woonplaatsen_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/woonplaatsen.json")


@pytest.fixture
def woningbouwplannen_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/woningbouwplannen.json")


@pytest.fixture
def brp_r_profile_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRP_R.json")


@pytest.fixture
def profile_brk_encoded_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRK_encoded.json")


@pytest.fixture
def profile_brk_read_id_schema(here) -> ProfileSchema:
    return ProfileSchema.from_file(here / "files/profiles/BRK_RID.json")


@pytest.fixture
def composite_key_schema(here) -> ProfileSchema:
    return dataset_schema_from_path(here / "files/composite_key.json")


@pytest.fixture
def profile_verkeer_medewerker_schema() -> ProfileSchema:
    return ProfileSchema.from_dict(
        {
            "name": "verkeer_medewerker",
            "scopes": ["FP/MD"],
            "datasets": {
                "verkeer": {},  # needed to be applied to a dataset.
            },
        }
    )


@pytest.fixture
def brk_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/brk.json")


@pytest.fixture
def hr_schema(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/hr.json")


@pytest.fixture
def hr_schema_auth(here) -> DatasetSchema:
    return dataset_schema_from_path(here / "files/hr_auth.json")
