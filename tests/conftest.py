from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager
from urllib.parse import ParseResult, urlparse

import pytest
import requests
import sqlalchemy_utils
from more_ds.network.url import URL
from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.sql.ddl import DropTable

from schematools.importer.base import metadata
from schematools.loaders import FileSystemProfileLoader, FileSystemSchemaLoader
from schematools.types import DatasetSchema, Json, ProfileSchema

HERE = Path(__file__).parent


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
    dummy_session_maker = DummySessionMaker()

    AFVALWEGINGEN_JSON = HERE / "files/datasets/afvalwegingen_sep_table.json"
    CLUSTERS_JSON = HERE / "files/datasets/afvalwegingen_clusters/v1.0.0.json"
    VERBLIJFSOBJECTEN_JSON = HERE / "files/datasets/verblijfsobjecten.json"

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
            schema_url / "afvalwegingen/afvalwegingen_clusters/v1.0.0",
            content=json.load(fh),
        )
    yield dummy_session_maker


@pytest.fixture
def afval_schema_json(here: Path) -> Json:
    with open(here / "files/datasets/afval.json") as f:
        return json.load(f)


@pytest.fixture()
def schema_loader(here) -> FileSystemSchemaLoader:
    """A single schema loader instance that is shared by a single test run.
    This also means all fixtures of a single test share the same dataset_collection,
    as this schema loader assigns that to each loaded dataset.
    """
    return FileSystemSchemaLoader(here / "files/datasets")


@pytest.fixture
def aardgasverbruik_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("aardgasverbruik.json")


@pytest.fixture
def afval_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("afval.json")


@pytest.fixture
def afvalwegingen_schema(schema_loader, verblijfsobjecten_schema) -> DatasetSchema:
    # verblijfsobjecten_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("afvalwegingen.json")


@pytest.fixture
def brk_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("brk.json")


@pytest.fixture
def brk_schema_without_bag_relations(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("brk_without_bag_relations.json")


@pytest.fixture
def brk2_simple_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("brk2_simple.json")


@pytest.fixture
def composite_key_schema(schema_loader) -> ProfileSchema:
    return schema_loader.get_dataset_from_file("composite_key.json")


@pytest.fixture
def hr_schema(schema_loader, verblijfsobjecten_schema) -> DatasetSchema:
    # verblijfsobjecten_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("hr.json")


@pytest.fixture
def hr_simple_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("hr_simple.json")


@pytest.fixture
def hr_schema_auth(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("hr_auth.json")


@pytest.fixture
def meetbouten_schema(schema_loader, gebieden_schema) -> DatasetSchema:
    # gebieden_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("meetbouten.json")


@pytest.fixture
def parkeervakken_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("parkeervakken.json")


@pytest.fixture
def gebieden_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("gebieden.json")


@pytest.fixture
def nap_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("nap.json")


@pytest.fixture
def benk_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("benk.json")


@pytest.fixture
def bouwblokken_schema(schema_loader, gebieden_schema) -> DatasetSchema:
    # gebieden_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("bouwblokken.json")


@pytest.fixture
def gebieden_schema_auth(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("gebieden_auth.json")


@pytest.fixture
def gebieden_schema_auth_list(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("gebieden_auth_list.json")


@pytest.fixture
def ggwgebieden_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("ggwgebieden.json")


@pytest.fixture
def stadsdelen_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("stadsdelen.json")


@pytest.fixture
def verblijfsobjecten_schema(schema_loader, gebieden_schema) -> DatasetSchema:
    # gebieden_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("verblijfsobjecten.json")


@pytest.fixture
def bag_verblijfsobjecten_schema(schema_loader, gebieden_schema) -> DatasetSchema:
    # gebieden_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("bag_verblijfsobjecten.json")


@pytest.fixture
def kadastraleobjecten_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("kadastraleobjecten.json")


@pytest.fixture
def meldingen_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("meldingen.json")


@pytest.fixture
def woonplaatsen_schema(schema_loader) -> DatasetSchema:
    return schema_loader.get_dataset_from_file("woonplaatsen.json")


@pytest.fixture
def woningbouwplannen_schema(schema_loader, gebieden_schema) -> DatasetSchema:
    # gebieden_schema is listed as dependency to resolve relations
    return schema_loader.get_dataset_from_file("woningbouwplannen.json")


@pytest.fixture()
def profile_loader(here) -> FileSystemProfileLoader:
    return FileSystemProfileLoader(here / "files/profiles")


@pytest.fixture
def brp_r_profile_schema(profile_loader) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return profile_loader.get_profile("BRP_R")


@pytest.fixture
def profile_brk_encoded_schema(profile_loader) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return profile_loader.get_profile("BRK_encoded")


@pytest.fixture
def profile_brk_read_id_schema(profile_loader) -> ProfileSchema:
    return profile_loader.get_profile("BRK_RID")


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
