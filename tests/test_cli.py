from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from schematools.cli import (
    batch_validate,
    validate_datasets,
    validate_publishers,
    validate_scopes,
    validate_tables,
)


class Publisher(SimpleNamespace):
    pass


class Scope(SimpleNamespace):
    pass


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
