from __future__ import annotations

import os

import pytest

from schematools.exceptions import DuplicateScopeId
from schematools.loaders import URLSchemaLoader
from schematools.types import Scope


def test_load_all_publishers(schema_loader):
    pubs = schema_loader.get_all_publishers()
    assert pubs == {
        "GLEBZ": {
            "id": "GLEBZ",
            "name": "Datateam Glebz",
            "shortname": "braft",
            "tags": {"costcenter": "12345.6789"},
        },
        "HARRY": {
            "id": "HARRY",
            "name": "Datateam Harry",
            "shortname": "harhar",
            "tags": {"costcenter": "123456789.4321.13519", "team": "taggy"},
        },
        "NOTTHESAMEASFILENAME": {
            "id": "NOTTHESAMEASFILENAME",
            "name": "Datateam incorrect",
            "shortname": "nono",
            "tags": {"costcenter": "1236789.4321.13519", "team": "taggy"},
        },
    }


def test_publisher_url():
    loader = URLSchemaLoader("https://foo.bar/baz/datasets/")
    assert loader._get_publisher_url() == "https://foo.bar/baz/publishers"


GLEBZ_SCOPE = Scope(
    {
        "name": "GLEBZscope",
        "id": "GLEBZ",
        "accessPackages": {
            "production": "EM4W-DATA-schemascope-p-scope_glebz",
            "nonProduction": "EM4W-DATA-schemascope-ot-scope_glebz",
        },
        "owner": {"$ref": "publishers/GLEBZ"},
    }
)
HARRY_ONE_SCOPE = Scope(
    {
        "name": "HARRYscope1",
        "id": "HARRY/ONE",
        "accessPackages": {
            "production": "EM4W-DATA-schemascope-p-scope_harry_one",
            "nonProduction": "EM4W-DATA-schemascope-ot-scope_harry_one",
        },
        "owner": {"$ref": "publishers/HARRY"},
    }
)
HARRY_TWO_SCOPE = Scope(
    {
        "name": "HARRYscope2",
        "id": "HARRY/TWO",
        "accessPackages": {
            "production": "EM4W-DATA-schemascope-p-scope_harry_two",
            "nonProduction": "EM4W-DATA-schemascope-ot-scope_harry_two",
        },
        "owner": {"$ref": "publishers/HARRY"},
    }
)
HARRY_THREE_SCOPE = Scope(
    {
        "name": "HARRYscope3",
        "id": "HARRY/THREE",
        "accessPackages": {
            "production": "EM4W-DATA-schemascope-p-scope_harry_three",
            "nonProduction": "EM4W-DATA-schemascope-ot-scope_harry_three",
        },
        "owner": {"$ref": "publishers/HARRY"},
    }
)


def test_load_all_scopes_file_loader(schema_loader):
    scopes = schema_loader.get_all_scopes()
    # Unclear why this needs the Scope() objects, while the test_load_all_publishers
    # test does not need the Publisher() objects.
    assert scopes == {
        "glebz": GLEBZ_SCOPE,
        "harry_one": HARRY_ONE_SCOPE,
        "harry_two": HARRY_TWO_SCOPE,
        "harry_three": HARRY_THREE_SCOPE,
    }


@pytest.mark.xfail(raises=DuplicateScopeId, strict=True)
def test_load_all_scopes_fails_on_duplicates(schema_loader_duplicate_scope):
    schema_loader_duplicate_scope.get_all_scopes()


# Skipping the following test by default, because it can be sloooow
# run this by adding `export ONLY_LOCAL=0;` before the pytest command
@pytest.mark.skipif(
    os.environ.get("ONLY_LOCAL", True),
    reason="Not running because it depends on external service.",
)
def test_load_all_scopes_url_loader():
    SCHEMA_URL = "http://schemas.data.amsterdam.nl/datasets/"
    loader = URLSchemaLoader(SCHEMA_URL)
    scopes = loader.get_all_scopes()

    assert "openbaar" in scopes

    openbaar = scopes["openbaar"]
    assert isinstance(openbaar, Scope)
    assert openbaar.id == "OPENBAAR"
    assert openbaar.accessPackages != {}
    assert openbaar.productionPackage != ""
    assert openbaar.nonProductionPackage != ""
