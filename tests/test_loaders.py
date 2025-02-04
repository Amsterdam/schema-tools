from __future__ import annotations

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


def test_load_all_scopes(schema_loader):
    scopes = schema_loader.get_all_scopes()
    # Unclear why this needs the Scope() objects, while the test_load_all_publishers
    # test does not need the Publisher() objects.
    assert scopes == {
        "GLEBZ": Scope(
            {
                "name": "GLEBZscope",
                "id": "GLEBZ",
                "owner": {"$ref": "publishers/GLEBZ"},
            }
        ),
        "HARRY/ONE": Scope(
            {
                "name": "HARRYscope1",
                "id": "HARRY/ONE",
                "owner": {"$ref": "publishers/HARRY"},
            }
        ),
        "HARRY/TWO": Scope(
            {
                "name": "HARRYscope2",
                "id": "HARRY/TWO",
                "owner": {"$ref": "publishers/HARRY"},
            }
        ),
        "HARRY/THREE": Scope(
            {
                "name": "HARRYscope3",
                "id": "HARRY/THREE",
                "owner": {"$ref": "publishers/HARRY"},
            }
        ),
    }


@pytest.mark.xfail(raises=DuplicateScopeId, strict=True)
def test_load_all_scopes_fails_on_duplicates(schema_loader_duplicate_scope):
    schema_loader_duplicate_scope.get_all_scopes()
