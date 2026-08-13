from __future__ import annotations

import builtins
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import click
import jsonschema
import pytest
from click.testing import CliRunner

import schematools
from schematools import validation
from schematools.cli import (
    ValidationIssue,
    batch_validate,
    permissions_apply,
    schema,
    validate_datasets,
    validate_publishers,
    validate_scopes,
    validate_tables,
)


class Publisher(SimpleNamespace):
    pass


class Scope(SimpleNamespace):
    pass


def test_cli_import_does_not_require_databricks_sdk(monkeypatch) -> None:
    original_import = builtins.__import__
    original_cli_attr = getattr(schematools, "cli", None)

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("databricks"):
            raise ModuleNotFoundError("No module named 'databricks'", name=name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    sys.modules.pop("schematools.cli", None)

    try:
        cli = importlib.import_module("schematools.cli")
    finally:
        if original_cli_attr is None:
            delattr(schematools, "cli")
        else:
            schematools.cli = original_cli_attr

    assert cli is not None


def test_validation_issue_helpers() -> None:
    issue = ValidationIssue("plain message")

    assert issue.message == "plain message"
    assert issue.as_markdown_todo() == "- [ ] plain message"
    assert ValidationIssue.from_string("boom").message == "boom"
    assert (
        ValidationIssue.from_validation_error(
            validation.ValidationError(validator_name="demo", message="broken")
        ).message
        == "[demo] broken"
    )
    assert ValidationIssue.from_exception(RuntimeError("kapot")).message == "kapot"


def test_validation_issue_from_jsonschema_error_formats_message() -> None:
    error = next(
        jsonschema.Draft7Validator(
            {
                "type": "object",
                "required": ["id"],
            }
        ).iter_errors({})
    )

    issue = ValidationIssue.from_jsonschema_error(error)

    assert issue.message == "$: 'id' is a required property"


def test_fetch_json_reads_local_schema_file_from_directory(tmp_path: Path) -> None:
    schema_dir = tmp_path / "example"
    schema_dir.mkdir()
    (schema_dir / "schema.json").write_text(json.dumps({"id": "example"}))

    assert schematools.cli._fetch_json(str(schema_dir)) == {"id": "example"}


def test_fetch_json_reads_remote_json(monkeypatch) -> None:
    request_calls = []

    class Response:
        def raise_for_status(self):
            request_calls.append("raise_for_status")

        def json(self):
            return {"id": "remote"}

    def fake_get(url, timeout):
        request_calls.append((url, timeout))
        return Response()

    monkeypatch.setattr("schematools.cli.requests.get", fake_get)

    assert schematools.cli._fetch_json("https://example.test/schema.json") == {"id": "remote"}
    assert request_calls == [("https://example.test/schema.json", 60), "raise_for_status"]


def test_get_dataset_schema_translates_dataset_not_found(monkeypatch) -> None:
    def raise_not_found(*_args, **_kwargs):
        raise schematools.cli.DatasetNotFound("dataset missing")

    monkeypatch.setattr(
        "schematools.cli.get_schema_loader",
        lambda _url: SimpleNamespace(get_dataset=raise_not_found),
    )

    with pytest.raises(click.ClickException, match="dataset missing"):
        schematools.cli._get_dataset_schema("missing", "https://schemas.example.test")


def test_get_publishers_translates_missing_schema_object(monkeypatch) -> None:
    def raise_not_found():
        raise schematools.cli.SchemaObjectNotFound("publishers missing")

    monkeypatch.setattr(
        "schematools.cli.get_schema_loader",
        lambda _url: SimpleNamespace(get_all_publishers=raise_not_found),
    )

    with pytest.raises(click.ClickException, match="publishers missing"):
        schematools.cli._get_publishers("https://schemas.example.test")


def test_get_scopes_translates_duplicate_scope_id(monkeypatch) -> None:
    def raise_duplicate():
        raise schematools.cli.DuplicateScopeId("duplicate scope")

    monkeypatch.setattr(
        "schematools.cli.get_schema_loader",
        lambda _url: SimpleNamespace(get_all_scopes=raise_duplicate),
    )

    with pytest.raises(click.ClickException, match="duplicate scope"):
        schematools.cli._get_scopes("https://schemas.example.test")


def test_get_databricks_info_requires_optional_dependency(monkeypatch) -> None:
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "schematools.contrib.databricks.client":
            raise ModuleNotFoundError("No module named 'databricks.sdk'", name="databricks.sdk")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    with pytest.raises(
        click.ClickException, match="requires the optional databricks dependencies"
    ):
        schematools.cli._get_databricks_info("main", "default", "cafes")


def test_permissions_apply_requires_auto_or_role_and_scope(monkeypatch) -> None:
    monkeypatch.setattr("schematools.cli._get_engine", lambda _db_url: object())
    monkeypatch.setattr(
        "schematools.cli.get_schema_loader",
        lambda _url: SimpleNamespace(
            get_all_datasets=dict,
            get_all_scopes=dict,
        ),
    )
    monkeypatch.setattr(
        "schematools.cli.get_profile_loader",
        lambda _url: SimpleNamespace(get_all_profiles=list),
    )

    apply_calls = []
    monkeypatch.setattr(
        "schematools.cli.apply_schema_and_profile_permissions",
        lambda *args, **kwargs: apply_calls.append((args, kwargs)),
    )

    result = CliRunner().invoke(permissions_apply, ["--db-url", "postgresql://example/db"])

    assert result.exit_code == 0
    assert (
        "Choose --auto or specify both a --role and a --scope to be able to grant permissions"
        in result.stdout
    )
    assert apply_calls == []


def test_permissions_apply_rejects_destructive_partial_revoke(monkeypatch) -> None:
    monkeypatch.setattr("schematools.cli._get_engine", lambda _db_url: object())
    monkeypatch.setattr(
        "schematools.cli.get_schema_loader",
        lambda _url: SimpleNamespace(
            get_all_datasets=dict,
            get_all_scopes=dict,
        ),
    )
    monkeypatch.setattr(
        "schematools.cli.get_profile_loader",
        lambda _url: SimpleNamespace(get_all_profiles=list),
    )

    apply_calls = []
    monkeypatch.setattr(
        "schematools.cli.apply_schema_and_profile_permissions",
        lambda *args, **kwargs: apply_calls.append((args, kwargs)),
    )

    result = CliRunner().invoke(
        permissions_apply,
        [
            "--db-url",
            "postgresql://example/db",
            "--role",
            "scope_reader",
            "--scope",
            "scope",
            "--revoke",
            "--no-write",
        ],
    )

    assert result.exit_code == 0
    assert (
        "Using --revoke without setting both read and write permissions is destructive."
        in result.stdout
    )
    assert apply_calls == []


def test_permissions_apply_uses_local_schema_and_profile_files(
    monkeypatch, tmp_path: Path
) -> None:
    engine = object()
    dataset_schema = SimpleNamespace(id="dataset")
    scope_one = SimpleNamespace(id="scope_one")
    scope_two = SimpleNamespace(id="scope_two")
    local_loader = SimpleNamespace(
        get_dataset_from_file=lambda _path: dataset_schema,
        get_all_scopes=lambda: {
            "scope_one": scope_one,
            "scope_two": scope_two,
        },
    )
    profile = SimpleNamespace(id="profile")

    monkeypatch.setattr("schematools.cli._get_engine", lambda _db_url: engine)
    monkeypatch.setattr(
        "schematools.cli.FileSystemSchemaLoader",
        SimpleNamespace(from_file=lambda _path: local_loader),
    )
    monkeypatch.setattr(
        "schematools.cli.ProfileSchema",
        SimpleNamespace(from_file=lambda _path: profile),
    )

    apply_calls = []
    monkeypatch.setattr(
        "schematools.cli.apply_schema_and_profile_permissions",
        lambda *args, **kwargs: apply_calls.append((args, kwargs)),
    )

    schema_file = tmp_path / "dataset.json"
    profile_file = tmp_path / "profile.json"

    result = CliRunner().invoke(
        permissions_apply,
        [
            "--db-url",
            "postgresql://example/db",
            "--schema-filename",
            str(schema_file),
            "--profile-filename",
            str(profile_file),
            "--auto",
            "--execute",
            "--create-roles",
            "-v",
            "-a",
            "my_table:SELECT;consumer",
        ],
    )

    assert result.exit_code == 0
    assert len(apply_calls) == 1

    args, kwargs = apply_calls[0]
    assert args[0] is engine
    assert args[1] == {"dataset": dataset_schema}
    assert args[2] == [profile]
    assert kwargs["only_role"] is None
    assert kwargs["only_scope"] is None
    assert kwargs["set_read_permissions"] is True
    assert kwargs["set_write_permissions"] is True
    assert kwargs["dry_run"] is False
    assert kwargs["create_roles"] is True
    assert kwargs["revoke"] is False
    assert kwargs["verbose"] == 1
    assert kwargs["additional_grants"] == ("my_table:SELECT;consumer",)
    assert list(kwargs["all_scopes"]) == [scope_one, scope_two]


def test_validate_tables_aggregates_errors_on_stderr(tmp_path: Path) -> None:
    previous_table = tmp_path / "previous-table.json"
    current_table = tmp_path / "table.json"
    previous_table.write_text(
        json.dumps(
            {
                "id": "test",
                "version": "1.0.0",
                "schema": {"properties": {"field": {"type": "string"}}},
            }
        )
    )
    current_table.write_text(
        json.dumps(
            {
                "id": "test",
                "version": "1.0.0",
                "schema": {"properties": {}},
            }
        )
    )

    runner = CliRunner()
    result = runner.invoke(validate_tables, [str(current_table)])

    assert result.exit_code == 1
    assert "## Tables Validation Errors" in result.stderr
    assert f"### {current_table}" in result.stderr
    assert "- [ ] Column field would be deleted." in result.stderr
    assert "FAIL" in result.stdout


def test_validate_datasets_aggregates_errors_on_stderr(tmp_path: Path) -> None:
    previous_dataset = tmp_path / "previous-dataset.json"
    current_dataset = tmp_path / "dataset.json"
    previous_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.0.0",
                        "status": "stable",
                        "tables": [{"id": "table1", "$ref": "table/v1.0.0"}],
                    }
                },
            }
        )
    )
    current_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.0.0",
                        "status": "stable",
                        "tables": [],
                    }
                },
            }
        )
    )

    runner = CliRunner()
    result = runner.invoke(validate_datasets, [str(current_dataset)])

    assert result.exit_code == 1
    assert "## Datasets Validation Errors" in result.stderr
    assert f"### {current_dataset}" in result.stderr
    assert "- [ ] Table table1 has been removed." in result.stderr
    assert "FAIL" in result.stdout


def test_batch_validate_aggregates_errors_on_stderr(tmp_path: Path, monkeypatch) -> None:
    dataset_dir = tmp_path / "datasets" / "example"
    dataset_dir.mkdir(parents=True)
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text("{}")

    meta_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "version": {"type": "integer"}},
        "required": ["id", "version"],
    }
    dataset = SimpleNamespace(json_data=lambda **_kwargs: {})
    loader = SimpleNamespace(get_dataset_from_file=lambda _path: dataset)

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli.FileSystemSchemaLoader", lambda _path: loader)
    monkeypatch.setattr("schematools.cli.validation.run", lambda *_args, **_kwargs: [])

    runner = CliRunner()
    result = runner.invoke(batch_validate, ["schema@v4.2.0", str(dataset_file)])

    assert result.exit_code == 1
    assert "## Dataset Schema Validation Errors" in result.stderr
    assert f"### {dataset_file}" in result.stderr
    assert "- [ ] $: 'id' is a required property" in result.stderr
    assert "- [ ] $: 'version' is a required property" in result.stderr
    assert f"Validating {dataset_file} against 4.2.0" in result.stdout


def test_batch_validate_reports_nested_anyof_errors(tmp_path: Path, monkeypatch) -> None:
    dataset_dir = tmp_path / "datasets" / "example"
    dataset_dir.mkdir(parents=True)
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text("{}")

    meta_schema = {
        "type": "object",
        "anyOf": [
            {"required": ["id"]},
            {"required": ["version"]},
        ],
    }
    dataset = SimpleNamespace(json_data=lambda **_kwargs: {})
    loader = SimpleNamespace(get_dataset_from_file=lambda _path: dataset)

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli.FileSystemSchemaLoader", lambda _path: loader)
    monkeypatch.setattr("schematools.cli.validation.run", lambda *_args, **_kwargs: [])

    runner = CliRunner()
    result = runner.invoke(batch_validate, ["schema@v4.2.0", str(dataset_file)])

    assert result.exit_code == 1
    assert "- [ ] $: 'id' is a required property" in result.stderr
    assert "- [ ] $: 'version' is a required property" in result.stderr
    assert "is not valid under any of the given schemas" not in result.stderr


def test_batch_validate_prefers_deeper_anyof_errors(tmp_path: Path, monkeypatch) -> None:
    dataset_dir = tmp_path / "datasets" / "example"
    dataset_dir.mkdir(parents=True)
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text("{}")

    meta_schema = {
        "type": "object",
        "anyOf": [
            {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "object",
                        "properties": {"bar": {"type": "string"}},
                        "required": ["bar"],
                    }
                },
                "required": ["foo"],
            },
            {"type": "object", "required": ["baz"]},
        ],
    }
    dataset = SimpleNamespace(json_data=lambda **_kwargs: {"foo": {}})
    loader = SimpleNamespace(get_dataset_from_file=lambda _path: dataset)

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli.FileSystemSchemaLoader", lambda _path: loader)
    monkeypatch.setattr("schematools.cli.validation.run", lambda *_args, **_kwargs: [])

    runner = CliRunner()
    result = runner.invoke(batch_validate, ["schema@v4.2.0", str(dataset_file)])

    assert result.exit_code == 1
    assert "'bar' is a required property" in result.stderr
    assert "'baz' is a required property" not in result.stderr


def test_validate_tables_does_not_write_error_header_without_errors(tmp_path: Path) -> None:
    previous_table = tmp_path / "previous-table.json"
    current_table = tmp_path / "table.json"
    table_data = {
        "id": "test",
        "version": "1.0.0",
        "schema": {"properties": {"field": {"type": "string"}}},
    }
    previous_table.write_text(json.dumps(table_data))
    current_table.write_text(json.dumps(table_data))

    runner = CliRunner()
    result = runner.invoke(validate_tables, [str(current_table)])

    assert result.exit_code == 0
    assert result.stderr == ""
    assert "## Tables Validation Errors" not in result.output


def test_validate_datasets_does_not_write_error_header_without_errors(tmp_path: Path) -> None:
    previous_dataset = tmp_path / "previous-dataset.json"
    current_dataset = tmp_path / "dataset.json"
    previous_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.0.0",
                        "status": "stable",
                        "tables": [{"id": "table1", "$ref": "table/v1.0.0"}],
                    }
                },
            }
        )
    )
    current_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.1.0",
                        "status": "stable",
                        "tables": [
                            {"id": "table1", "$ref": "table/v1.0.0"},
                            {"id": "table2", "$ref": "table/v1.0.0"},
                        ],
                    }
                },
            }
        )
    )

    runner = CliRunner()
    result = runner.invoke(validate_datasets, [str(current_dataset)])

    assert result.exit_code == 0
    assert result.stderr == ""
    assert "## Datasets Validation Errors" not in result.output


def test_validate_datasets_skips_missing_and_under_development_versions(tmp_path: Path) -> None:
    previous_dataset = tmp_path / "previous-dataset.json"
    current_dataset = tmp_path / "dataset.json"
    previous_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.0.0",
                        "status": "under_development",
                        "tables": [],
                    }
                },
            }
        )
    )
    current_dataset.write_text(
        json.dumps(
            {
                "id": "dataset",
                "versions": {
                    "v1": {
                        "version": "1.1.0",
                        "status": "stable",
                        "tables": [],
                    },
                    "v2": {
                        "version": "2.0.0",
                        "status": "stable",
                        "tables": [],
                    },
                },
            }
        )
    )

    result = CliRunner().invoke(validate_datasets, [str(current_dataset)])

    assert result.exit_code == 0
    assert "Dataset has no previous version" in result.stdout
    assert "Breaking changes detected" not in result.output


def test_validate_tables_skips_under_development_previous_version(tmp_path: Path) -> None:
    previous_table = tmp_path / "previous-table.json"
    current_table = tmp_path / "table.json"
    previous_table.write_text(
        json.dumps(
            {
                "status": "under_development",
                "schema": {"properties": {"field": {"type": "string"}}},
            }
        )
    )
    current_table.write_text("{}")

    result = CliRunner().invoke(validate_tables, [str(current_table)])

    assert result.exit_code == 0
    assert result.stderr == ""
    assert "All tables are backwards compatible" in result.stdout


def test_validate_tables_reports_malformed_json_file(tmp_path: Path) -> None:
    previous_table = tmp_path / "previous-table.json"
    current_table = tmp_path / "table.json"
    previous_table.write_text(
        json.dumps(
            {
                "status": "stable",
                "schema": {"properties": {"field": {"type": "string"}}},
            }
        )
    )
    current_table.write_text(json.dumps({"status": "stable"}))

    result = CliRunner().invoke(validate_tables, [str(current_table)])

    assert result.exit_code == 1
    assert "FAIL" in result.stdout
    assert "Malformed json-file." in result.stderr


def test_batch_validate_rejects_files_outside_datasets_dir(tmp_path: Path) -> None:
    schema_file = tmp_path / "dataset.json"
    schema_file.write_text("{}")

    result = CliRunner().invoke(batch_validate, ["schema@v4.2.0", str(schema_file)])

    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "dataset files do not live in a common 'datasets' dir"


def test_validate_publishers_aggregates_errors_on_stderr(monkeypatch) -> None:
    meta_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    publishers = {"publisher-one": Publisher(json_data=dict)}

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli._get_publishers", lambda _url: publishers)

    runner = CliRunner()
    result = runner.invoke(
        validate_publishers,
        ["--schema-url", "https://schemas.data.amsterdam.nl/datasets/", "schema@v4.0.0"],
    )

    assert result.exit_code == 1
    assert "## Publishers Validation Errors" in result.stderr
    assert "### publisher-one" in result.stderr
    assert "- [ ] $: 'id' is a required property" in result.stderr
    assert "Validating Publisher with id publisher-one" in result.stdout


def test_validate_publishers_does_not_write_error_header_without_errors(monkeypatch) -> None:
    meta_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    publishers = {"publisher-one": Publisher(json_data=lambda: {"id": "publisher-one"})}

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli._get_publishers", lambda _url: publishers)

    runner = CliRunner()
    result = runner.invoke(
        validate_publishers,
        ["--schema-url", "https://schemas.data.amsterdam.nl/datasets/", "schema@v4.0.0"],
    )

    assert result.exit_code == 0
    assert result.stderr == ""
    assert "## Publishers Validation Errors" not in result.output


def test_validate_scopes_aggregates_errors_on_stderr(monkeypatch) -> None:
    meta_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    scopes = {"scope-one": Scope(json_data=dict)}

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli._get_scopes", lambda _url: scopes)

    runner = CliRunner()
    result = runner.invoke(
        validate_scopes,
        ["--schema-url", "https://schemas.data.amsterdam.nl/datasets/", "schema@v4.0.0"],
    )

    assert result.exit_code == 1
    assert "## Scopes Validation Errors" in result.stderr
    assert "### scope-one" in result.stderr
    assert "- [ ] $: 'id' is a required property" in result.stderr
    assert "Validating Scope with id scope-one" in result.stdout


def test_validate_scopes_does_not_write_error_header_without_errors(monkeypatch) -> None:
    meta_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    scopes = {"scope-one": Scope(json_data=lambda: {"id": "scope-one"})}

    monkeypatch.setattr("schematools.cli._fetch_json", lambda _url: meta_schema)
    monkeypatch.setattr("schematools.cli._get_scopes", lambda _url: scopes)

    runner = CliRunner()
    result = runner.invoke(
        validate_scopes,
        ["--schema-url", "https://schemas.data.amsterdam.nl/datasets/", "schema@v4.0.0"],
    )

    assert result.exit_code == 0
    assert result.stderr == ""
    assert "## Scopes Validation Errors" not in result.output


def test_to_ckan_requires_api_key_when_uploading(monkeypatch) -> None:
    monkeypatch.delenv("CKAN_API_KEY", raising=False)

    runner = CliRunner()
    result = runner.invoke(
        schema,
        [
            "ckan",
            "--schema-url",
            "https://schemas.data.amsterdam.nl/datasets/",
            "--upload-url",
            "https://data.example.test",
        ],
    )

    assert result.exit_code == 1
    assert "CKAN_API_KEY not set in environment" in result.stderr


def test_to_ckan_retries_package_create_after_404(monkeypatch) -> None:
    monkeypatch.setattr(
        "schematools.cli.DatasetSchema",
        SimpleNamespace(Status=SimpleNamespace(beschikbaar="published")),
    )
    dataset = SimpleNamespace(
        status="published",
        identifier="cafes",
    )
    skipped_dataset = SimpleNamespace(
        status="hidden",
        identifier="hidden",
    )
    loader = SimpleNamespace(
        get_all_datasets=lambda: {
            "datasets/cafes": dataset,
            "datasets/hidden": skipped_dataset,
        }
    )
    request_calls: list[tuple[str, dict, dict, int]] = []

    class Response:
        def __init__(self, status_code: int, payload: dict[str, object]):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    responses = iter(
        [
            Response(404, {"success": False}),
            Response(201, {"success": True}),
        ]
    )

    monkeypatch.setenv("CKAN_API_KEY", "secret")
    monkeypatch.setattr("schematools.cli.get_schema_loader", lambda _url: loader)
    monkeypatch.setattr(
        "schematools.cli.ckan.from_dataset",
        lambda ds, path: {"identifier": ds.identifier, "path": path},
    )

    def fake_post(url, headers, json, timeout):
        request_calls.append((url, headers, json, timeout))
        return next(responses)

    monkeypatch.setattr("schematools.cli.requests.post", fake_post)

    runner = CliRunner()
    result = runner.invoke(
        schema,
        [
            "ckan",
            "--schema-url",
            "https://schemas.data.amsterdam.nl/datasets/",
            "--upload-url",
            "https://data.example.test",
        ],
    )

    assert result.exit_code == 0
    assert len(request_calls) == 2
    assert request_calls[0][0] == "https://data.example.test/api/3/action/package_update?id=cafes"
    assert request_calls[1][0] == "https://data.example.test/api/3/action/package_create"
    assert request_calls[0][1] == {"Authorization": "secret"}
    assert request_calls[0][2] == {"identifier": "cafes", "path": "datasets/cafes"}


def test_to_ckan_returns_error_for_non_successful_upload(monkeypatch) -> None:
    monkeypatch.setattr(
        "schematools.cli.DatasetSchema",
        SimpleNamespace(Status=SimpleNamespace(beschikbaar="published")),
    )
    dataset = SimpleNamespace(status="published", identifier="cafes")
    loader = SimpleNamespace(get_all_datasets=lambda: {"datasets/cafes": dataset})
    request_calls: list[tuple[str, dict, dict, int]] = []

    class Response:
        status_code = 500

        def json(self):
            return {"success": False, "error": "boom"}

    monkeypatch.setenv("CKAN_API_KEY", "secret")
    monkeypatch.setattr("schematools.cli.get_schema_loader", lambda _url: loader)
    monkeypatch.setattr(
        "schematools.cli.ckan.from_dataset",
        lambda ds, path: {"identifier": ds.identifier, "path": path},
    )

    def fake_post(url, headers, json, timeout):
        request_calls.append((url, headers, json, timeout))
        return Response()

    monkeypatch.setattr("schematools.cli.requests.post", fake_post)

    runner = CliRunner()
    result = runner.invoke(
        schema,
        [
            "ckan",
            "--schema-url",
            "https://schemas.data.amsterdam.nl/datasets/",
            "--upload-url",
            "https://data.example.test",
        ],
    )

    assert result.exit_code == 1
    assert len(request_calls) == 1
    assert request_calls[0][0] == "https://data.example.test/api/3/action/package_update?id=cafes"
    assert request_calls[0][1] == {"Authorization": "secret"}
    assert request_calls[0][2] == {"identifier": "cafes", "path": "datasets/cafes"}


def test_to_ckan_prints_successful_datasets_after_conversion_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        "schematools.cli.DatasetSchema",
        SimpleNamespace(Status=SimpleNamespace(beschikbaar="published")),
    )
    broken_dataset = SimpleNamespace(status="published", identifier="broken")
    working_dataset = SimpleNamespace(status="published", identifier="cafes")
    loader = SimpleNamespace(
        get_all_datasets=lambda: {
            "datasets/broken": broken_dataset,
            "datasets/cafes": working_dataset,
        }
    )

    monkeypatch.setattr("schematools.cli.get_schema_loader", lambda _url: loader)

    def fake_from_dataset(ds, path):
        if ds.identifier == "broken":
            raise RuntimeError("boom")
        return {"identifier": ds.identifier, "path": path}

    monkeypatch.setattr("schematools.cli.ckan.from_dataset", fake_from_dataset)

    result = CliRunner().invoke(
        schema,
        ["ckan", "--schema-url", "https://schemas.data.amsterdam.nl/datasets/"],
    )

    assert result.exit_code == 0
    assert "{'identifier': 'cafes', 'path': 'datasets/cafes'}" in result.stdout


def test_ingest_preserves_unicode_characters_in_written_files(tmp_path: Path, monkeypatch) -> None:
    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir()
    dataset_file = dataset_dir / "dataset.json"
    dataset_file.write_text(
        json.dumps(
            {
                "title": "Cafés",
                "versions": {
                    "v1": {
                        "tables": [{"provenance": "uc:main.default.cafes_table"}],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    db_info = SimpleNamespace(
        errors=[],
        table_id="cafes",
        dict={"version": "1.0.0"},
        json=json.dumps(
            {
                "id": "cafes",
                "schema": {"properties": {"name": {"description": "Café op de kade"}}},
            },
            indent=2,
            ensure_ascii=False,
        ),
    )
    monkeypatch.setattr("schematools.cli._get_databricks_info", lambda *_args: db_info)

    runner = CliRunner()
    result = runner.invoke(schema, ["ingest", str(dataset_file)])

    assert result.exit_code == 0
    assert "Cafés" in dataset_file.read_text(encoding="utf-8")
    assert "\\u00e9" not in dataset_file.read_text(encoding="utf-8")

    table_file = dataset_dir / "cafes" / "v1.json"
    assert "Café op de kade" in table_file.read_text(encoding="utf-8")
    assert "\\u00e9" not in table_file.read_text(encoding="utf-8")


def test_ingest_does_not_write_dataset_without_uc_provenance(tmp_path: Path, monkeypatch) -> None:
    dataset_dir = tmp_path / "datasets"
    dataset_dir.mkdir()
    dataset_file = dataset_dir / "dataset.json"
    original_content = json.dumps(
        {
            "title": "No Databricks",
            "versions": {
                "v1": {
                    "tables": [
                        {"provenance": "source-system.table"},
                        {"provenance": "foo:main.default.cafes_table"},
                        {},
                    ],
                }
            },
        },
        indent=2,
        ensure_ascii=False,
    )
    dataset_file.write_text(f"{original_content}\n", encoding="utf-8")

    def fail_if_called(*_args):
        raise AssertionError("_get_databricks_info should not be called without uc: provenance")

    monkeypatch.setattr("schematools.cli._get_databricks_info", fail_if_called)

    runner = CliRunner()
    result = runner.invoke(schema, ["ingest", str(dataset_file)])

    assert result.exit_code == 0
    assert dataset_file.read_text(encoding="utf-8") == f"{original_content}\n"
    assert not (dataset_dir / "cafes").exists()
