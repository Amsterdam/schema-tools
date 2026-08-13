from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import requests

from schematools.exceptions import (
    DatasetNotFound,
    DatasetTableNotFound,
    DuplicateProfileId,
    DuplicateScopeId,
    SchemaObjectNotFound,
)
from schematools.loaders import (
    FileSystemProfileLoader,
    FileSystemSchemaLoader,
    URLProfileLoader,
    URLSchemaLoader,
    _read_sql_path,
    _read_sql_url,
    get_profile_loader,
    get_schema_loader,
    read_json_path,
)
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


def test_filesystem_schema_loader_get_root_variants(tmp_path):
    repo_root = tmp_path / "repo"
    datasets_dir = repo_root / "datasets"
    dataset_dir = datasets_dir / "example"
    dataset_dir.mkdir(parents=True)
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text('{"type": "dataset", "id": "example", "versions": {}}')

    assert FileSystemSchemaLoader.get_root(datasets_dir) == datasets_dir
    assert FileSystemSchemaLoader.get_root(repo_root) == datasets_dir
    assert FileSystemSchemaLoader.get_root(dataset_file) == datasets_dir

    standalone = tmp_path / "standalone"
    standalone.mkdir()
    with pytest.raises(ValueError, match="No 'datasets' root found"):
        FileSystemSchemaLoader.get_root(standalone)


def test_get_dataset_from_file_resolves_relative_dataset_paths(tmp_path):
    repo_root = tmp_path / "repo"
    datasets_dir = repo_root / "datasets"
    dataset_dir = datasets_dir / "example"
    dataset_dir.mkdir(parents=True)
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text('{"type": "dataset", "id": "example", "versions": {}}')

    loader = FileSystemSchemaLoader(datasets_dir)

    dataset = loader.get_dataset_from_file(Path("datasets") / "example" / "dataset.json")

    assert dataset.id == "example"


def test_get_dataset_from_file_rejects_external_files(tmp_path):
    repo_root = tmp_path / "repo"
    datasets_dir = repo_root / "datasets"
    datasets_dir.mkdir(parents=True)
    loader = FileSystemSchemaLoader(datasets_dir)

    external_file = tmp_path / "outside.json"
    external_file.write_text('{"type": "dataset", "id": "outside", "versions": {}}')

    with pytest.raises(ValueError, match="does not exist in the schema repository"):
        loader.get_dataset_from_file(external_file)


def test_read_json_path_normalizes_invalid_and_missing_files(tmp_path):
    invalid_json_file = tmp_path / "invalid.json"
    invalid_json_file.write_text("{")

    with pytest.raises(ValueError, match="Invalid JSON file"):
        read_json_path(invalid_json_file)

    with pytest.raises(SchemaObjectNotFound, match="missing.json"):
        read_json_path(tmp_path / "missing.json")


def test_sql_helpers_return_none_for_missing_or_unreachable_sources(monkeypatch, tmp_path):
    assert _read_sql_path(tmp_path / "missing.sql") is None

    class Response:
        status_code = 500
        text = ""

        def raise_for_status(self):
            raise AssertionError("raise_for_status should not be called for non-200 responses")

    monkeypatch.setattr("schematools.loaders.requests.get", lambda _url, timeout: Response())
    assert _read_sql_url("https://schemas.example.test/dataset.sql") is None

    def raise_connection_error(_url, timeout):
        raise requests.exceptions.ConnectionError()

    monkeypatch.setattr("schematools.loaders.requests.get", raise_connection_error)
    assert _read_sql_url("https://schemas.example.test/dataset.sql") is None


def test_url_schema_loader_read_json_url_raises_for_404(monkeypatch):
    class Response:
        status_code = 404

        def raise_for_status(self):
            raise AssertionError("raise_for_status should not be called for 404 responses")

        def json(self):
            return {}

    monkeypatch.setattr("schematools.loaders.requests.get", lambda _url, timeout: Response())
    loader = URLSchemaLoader("https://schemas.example.test/datasets")

    with pytest.raises(
        SchemaObjectNotFound, match="https://schemas.example.test/datasets/example"
    ):
        loader._read_json_url("https://schemas.example.test/datasets/example")


def test_filesystem_schema_loader_uses_given_root_when_no_datasets_dir_exists(tmp_path):
    loader = FileSystemSchemaLoader(tmp_path)

    assert loader.root == tmp_path


def test_get_dataset_path_raises_dataset_not_found(tmp_path):
    loader = FileSystemSchemaLoader(tmp_path)
    loader.__dict__["_dataset_paths"] = {}

    with pytest.raises(DatasetNotFound, match="Dataset 'missing' not found"):
        loader.get_dataset_path("missing")


def test_get_table_translates_loader_errors(tmp_path, monkeypatch):
    loader = FileSystemSchemaLoader(tmp_path)
    dataset = type("DatasetStub", (), {"id": "example", "__str__": lambda self: "example"})()

    def raise_dataset_not_found(_dataset_id, _table_ref):
        raise DatasetNotFound("example")

    monkeypatch.setattr(loader, "_read_table", raise_dataset_not_found)
    with pytest.raises(RuntimeError, match="Can't determine path to dataset 'example'!"):
        loader._get_table(dataset, "v1")

    def raise_schema_not_found(_dataset_id, _table_ref):
        raise SchemaObjectNotFound("missing table")

    monkeypatch.setattr(loader, "_read_table", raise_schema_not_found)
    with pytest.raises(DatasetTableNotFound, match="Dataset 'example' has no table ref: 'v1'!"):
        loader._get_table(dataset, "v1")


def test_url_schema_loader_loads_publishers_from_index(monkeypatch):
    loader = URLSchemaLoader("https://schemas.example.test/datasets")

    def fake_read_json_url(url):
        url = str(url)
        if url.endswith("/publishers/index"):
            return ["PUB1", "PUB2"]
        if url.endswith("/publishers/PUB1"):
            return {"id": "PUB1", "name": "Publisher One", "shortname": "one", "tags": {}}
        if url.endswith("/publishers/PUB2"):
            return {"id": "PUB2", "name": "Publisher Two", "shortname": "two", "tags": {}}
        raise AssertionError(url)

    monkeypatch.setattr(loader, "_read_json_url", fake_read_json_url)

    publishers = loader._get_all_publishers()

    assert set(publishers) == {"PUB1", "PUB2"}
    assert publishers["PUB1"].name == "Publisher One"
    assert publishers["PUB2"]["shortname"] == "two"


def test_url_schema_loader_rejects_duplicate_scope_ids(monkeypatch):
    loader = URLSchemaLoader("https://schemas.example.test/datasets")

    def fake_read_json_url(url):
        url = str(url)
        if url.endswith("/scopes/index"):
            return {"team_one": ["dup"], "team_two": ["dup"]}
        if url.endswith("/scopes/team_one/dup"):
            return {
                "name": "Duplicate Scope",
                "id": "DUP",
                "accessPackages": {"production": "prod", "nonProduction": "nonprod"},
                "owner": {"$ref": "publishers/GLEBZ"},
            }
        raise AssertionError(url)

    monkeypatch.setattr(loader, "_read_json_url", fake_read_json_url)

    with pytest.raises(DuplicateScopeId, match='Scope ID "dup" is already used'):
        loader._get_all_scopes()


def test_filesystem_profile_loader_calls_loaded_callback(here):
    loaded = []
    loader = FileSystemProfileLoader(here / "files/profiles", loaded_callback=loaded.append)

    schema = loader.get_profile("BRP_RNAME")

    assert loaded == [schema]
    assert schema.name == "brp_medewerker"


def test_filesystem_profile_loader_skips_index_file(tmp_path, here):
    valid_profile = (here / "files/profiles/BRP_RNAME.json").read_text()
    (tmp_path / "BRP_RNAME.json").write_text(valid_profile)
    (tmp_path / "index.json").write_text("this should be ignored")

    loader = FileSystemProfileLoader(tmp_path)

    profiles = loader.get_all_profiles()

    assert len(profiles) == 1
    assert profiles[0].name == "brp_medewerker"


def test_url_profile_loader_rejects_duplicate_profile_ids(monkeypatch, here):
    loader = URLProfileLoader("https://profiles.example.test")
    profile_data = json.loads((here / "files/profiles/BRP_RNAME.json").read_text())

    def fake_read_json_url(url):
        url = str(url)
        if url.endswith("/index"):
            return {"team_one": ["one"], "team_two": ["two"]}
        if url.endswith(("/team_one/one", "/team_two/two")):
            return profile_data
        raise AssertionError(url)

    monkeypatch.setattr(loader, "_read_json_url", fake_read_json_url)

    with pytest.raises(DuplicateProfileId, match='Profile ID "brp_medewerker" is already used'):
        loader.get_all_profiles()


def test_loader_factories_pick_implementations_from_urls_and_paths(monkeypatch, tmp_path):
    calls = {}

    def fake_url_schema_loader(schema_url, **kwargs):
        calls["schema"] = ("url", schema_url, kwargs)
        return "url-schema-loader"

    def fake_filesystem_schema_loader(schema_url, **kwargs):
        calls["schema"] = ("file", schema_url, kwargs)
        return "filesystem-schema-loader"

    def fake_url_profile_loader(profiles_url, **kwargs):
        calls["profile"] = ("url", profiles_url, kwargs)
        return "url-profile-loader"

    def fake_filesystem_profile_loader(profiles_url, **kwargs):
        calls["profile"] = ("file", profiles_url, kwargs)
        return "filesystem-profile-loader"

    monkeypatch.setattr("schematools.loaders.URLSchemaLoader", fake_url_schema_loader)
    monkeypatch.setattr(
        "schematools.loaders.FileSystemSchemaLoader", fake_filesystem_schema_loader
    )
    monkeypatch.setattr("schematools.loaders.URLProfileLoader", fake_url_profile_loader)
    monkeypatch.setattr(
        "schematools.loaders.FileSystemProfileLoader", fake_filesystem_profile_loader
    )

    monkeypatch.setenv("SCHEMA_URL", "https://schemas.example.test/datasets")
    assert get_schema_loader() == "url-schema-loader"
    assert calls["schema"] == ("url", "https://schemas.example.test/datasets", {})

    assert get_schema_loader(str(tmp_path)) == "filesystem-schema-loader"
    assert calls["schema"] == ("file", str(tmp_path), {})

    monkeypatch.setenv("PROFILES_URL", "https://profiles.example.test")
    assert get_profile_loader() == "url-profile-loader"
    assert calls["profile"] == ("url", "https://profiles.example.test", {})

    assert get_profile_loader(str(tmp_path)) == "filesystem-profile-loader"
    assert calls["profile"] == ("file", str(tmp_path), {})
