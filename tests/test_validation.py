from __future__ import annotations
from collections import Callable

import pytest

from schematools.types import DatasetSchema
from schematools.validation import IdentPropRefsValidator, PsqlIdentifierLengthValidator


def test_PsqlIdentifierLengthValidator(schema_json: Callable[[str], dict]) -> None:
    dataset = DatasetSchema.from_dict(schema_json("long_ids.json"))
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    error = next(validator.validate())
    assert error

    dataset = DatasetSchema.from_dict(schema_json("stadsdelen.json"))
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())


def test_IdentPropRefsValidator(schema_json: Callable[[str], dict]) -> None:
    dataset = DatasetSchema.from_dict(schema_json("identifier_ref.json"))
    validator = IdentPropRefsValidator(dataset=dataset)
    error = next(validator.validate())
    assert error
    assert "foobar" in error.message

    dataset = DatasetSchema.from_dict(schema_json("stadsdelen.json"))
    validator = IdentPropRefsValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())
