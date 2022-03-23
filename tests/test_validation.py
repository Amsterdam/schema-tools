from __future__ import annotations

import copy
from pathlib import Path
from typing import cast

import pytest
from more_itertools import first

from schematools import validation
from schematools.types import Json, TableVersions
from schematools.utils import dataset_schema_from_path


def test_camelcase() -> None:
    for ident in ("camelCase", "camelCase100"):
        assert validation._camelcase_ident(ident) is None

    error = validation._camelcase_ident("")
    assert error is not None
    assert "empty identifier" in error

    for ident, suggest in (
        ("snake_case", "snakeCase"),
        ("camelCase_snake", "camelCaseSnake"),
        ("camel100camel", "camel100Camel"),
    ):
        error = validation._camelcase_ident(ident)
        assert error is not None
        assert error.endswith(f"suggestion: {suggest}")


def test_id_auth(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/id_auth.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error
    assert error.validator_name == "Auth on identifier field"
    assert """auth on field 'id'""" in error.message

    assert list(errors) == []


def test_postgres_identifier_length(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/long_ids.json")

    error = next(validation.run(dataset))
    assert error
    assert error.validator_name == "PostgreSQL identifier length"
    assert "absurdly_long" in error.message

    dataset = dataset_schema_from_path(here / "files/stadsdelen.json")
    with pytest.raises(StopIteration):
        # no validation error
        next(validation.run(dataset))


def test_identifier_properties(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/identifier_ref.json")
    error = next(validation.run(dataset))
    assert error
    assert "foobar" in error.message

    dataset = dataset_schema_from_path(here / "files/stadsdelen.json")
    with pytest.raises(StopIteration):
        # no validation error
        next(validation.run(dataset))


def test_active_versions(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    table_version = cast(TableVersions, first(dataset["tables"]))
    table_version.id = table_version.id.capitalize()
    error = next(validation.run(dataset))
    assert error
    assert "does not match with id" in error.message

    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    table_version = cast(TableVersions, first(dataset["tables"]))
    for version in table_version.active:
        incorrect_version = copy.deepcopy(version)
        incorrect_version.major += 1
        cast(dict[str, Json], table_version.active[version])["version"] = str(incorrect_version)
    error = next(validation.run(dataset))
    assert error
    assert "does not match with version number" in error.message

    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    with pytest.raises(StopIteration):
        # no validation error
        next(validation.run(dataset))


def test_main_geometry(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files" / "meetbouten.json")
    assert list(validation.run(dataset)) == []

    dataset.get_table_by_id("meetbouten")["schema"]["mainGeometry"] = "not_a_geometry"
    error = next(validation.run(dataset))
    assert "mainGeometry = 'not_a_geometry'" in error.message
    assert "Field 'not_a_geometry' does not exist" in error.message

    dataset.get_table_by_id("meetbouten")["schema"]["mainGeometry"] = "merkOmschrijving"
    error = next(validation.run(dataset))
    assert error.message == (
        "mainGeometry = 'merkOmschrijving' is not a geometry field, type = 'string'"
    )
