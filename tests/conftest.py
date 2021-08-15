import os
from pathlib import Path
from urllib.parse import ParseResult, urlparse

import pytest
import sqlalchemy_utils
from geoalchemy2 import Geometry  # NoQA, needed to make postgis work
from more_ds.network.url import URL
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


@pytest.fixture()
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
def schemas_mock(requests_mock: Mocker, schema_url: URL) -> Mocker:
    """Mock the requests to import schemas.

    This allows to run "schema import schema afvalwegingen".
    """
    # `requests_mock` is a fixture from the requests_mock package
    AFVALWEGINGEN_JSON = HERE / "files" / "afvalwegingen_sep_table.json"
    CLUSTERS_JSON = HERE / "files" / "afvalwegingen_clusters-table.json"
    requests_mock.get(schema_url / "index.json", json={"afvalwegingen": "afvalwegingen"})
    with open(AFVALWEGINGEN_JSON, "rb") as fh:
        requests_mock.get(
            schema_url / "afvalwegingen/dataset",
            content=fh.read(),
        )
    with open(CLUSTERS_JSON, "rb") as fh:
        requests_mock.get(
            schema_url / "afvalwegingen/afvalwegingen_clusters-table",
            content=fh.read(),
        )
    yield requests_mock


@pytest.fixture()
def afval_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/afval.json")


@pytest.fixture()
def meetbouten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/meetbouten.json")


@pytest.fixture()
def parkeervakken_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/parkeervakken.json")


@pytest.fixture()
def gebieden_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden.json")


@pytest.fixture()
def bouwblokken_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/bouwblokken.json")


@pytest.fixture()
def gebieden_schema_auth(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden_auth.json")


@pytest.fixture()
def gebieden_schema_auth_list(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/gebieden_auth_list.json")


@pytest.fixture()
def ggwgebieden_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/ggwgebieden.json")


@pytest.fixture()
def stadsdelen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/stadsdelen.json")


@pytest.fixture()
def verblijfsobjecten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/verblijfsobjecten.json")


@pytest.fixture()
def kadastraleobjecten_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/kadastraleobjecten.json")


@pytest.fixture()
def meldingen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/meldingen.json")


@pytest.fixture()
def woonplaatsen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/woonplaatsen.json")


@pytest.fixture()
def woningbouwplannen_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/woningbouwplannen.json")


@pytest.fixture()
def brp_r_profile_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRP_R.json")


@pytest.fixture()
def profile_brk_encoded_schema(here) -> ProfileSchema:
    """A downloaded profile schema definition"""
    return ProfileSchema.from_file(here / "files/profiles/BRK_encoded.json")


@pytest.fixture()
def profile_brk_read_id_schema(here) -> ProfileSchema:
    return ProfileSchema.from_file(here / "files/profiles/BRK_RID.json")


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


@pytest.fixture()
def brk_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/brk.json")


@pytest.fixture()
def hr_schema(here) -> DatasetSchema:
    return DatasetSchema.from_file(here / "files/hr.json")
