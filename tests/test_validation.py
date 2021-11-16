from __future__ import annotations

import pytest

from schematools.types import DatasetSchema
from schematools.utils import dataset_schema_from_path
from schematools.validation import IdentPropRefsValidator, PsqlIdentifierLengthValidator


def test_PsqlIdentifierLengthValidator(here) -> None:
    dataset = dataset_schema_from_path(here / "files/long_ids.json")
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    error = next(validator.validate())
    assert error

    dataset = dataset_schema_from_path(here / "files/stadsdelen.json")
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())


def test_IdentPropRefsValidator(here) -> None:
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
