from __future__ import annotations

import copy
from pathlib import Path
from typing import cast

import pytest
from more_itertools import first

from schematools.types import Json, TableVersions
from schematools.utils import dataset_schema_from_path
from schematools.validation import (
    ActiveVersionsValidator,
    IdentPropRefsValidator,
    PsqlIdentifierLengthValidator,
    ValidationError,
    _validate_camelcase,
)


def test_validate_camelcase():
    for ident in ("camelCase", "camelCase100"):
        assert _validate_camelcase(ident) is None

    assert isinstance(_validate_camelcase(""), ValidationError)

    for ident, suggest in (
        ("snake_case", "snakeCase"),
        ("camelCase_snake", "camelCaseSnake"),
        ("camel100camel", "camel100Camel"),
    ):
        error = _validate_camelcase(ident)
        assert isinstance(error, ValidationError)
        assert error.message.endswith(f"suggestion: {suggest}")


def test_PsqlIdentifierLengthValidator(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/long_ids.json")
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    error = next(validator.validate())
    assert error

    dataset = dataset_schema_from_path(here / "files/stadsdelen.json")
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())


def test_IdentPropRefsValidator(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/identifier_ref.json")
    validator = IdentPropRefsValidator(dataset=dataset)
    error = next(validator.validate())
    assert error
    assert "foobar" in error.message

    dataset = dataset_schema_from_path(here / "files/stadsdelen.json")
    validator = IdentPropRefsValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())


def test_ActiveVersionsValidator(here: Path) -> None:
    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    table_version = cast(TableVersions, first(dataset["tables"]))
    table_version.id = table_version.id.capitalize()
    validator = ActiveVersionsValidator(dataset=dataset)
    error = next(validator.validate())
    assert error
    assert "does not match with id" in error.message

    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    table_version = cast(TableVersions, first(dataset["tables"]))
    for version in table_version.active:
        incorrect_version = copy.deepcopy(version)
        incorrect_version.major += 1
        cast(dict[str, Json], table_version.active[version])["version"] = str(incorrect_version)
    validator = ActiveVersionsValidator(dataset=dataset)
    error = next(validator.validate())
    assert error
    assert "does not match with version number" in error.message

    dataset = dataset_schema_from_path(here / "files/gebieden_sep_tables/dataset.json")
    validator = ActiveVersionsValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())
