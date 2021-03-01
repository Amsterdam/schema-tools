import pytest

from schematools.types import DatasetSchema
from schematools.validation import PsqlIdentifierLengthValidator


def test_PsqlIdentifierLengthValidator(schema_json) -> None:
    dataset = DatasetSchema.from_dict(schema_json("long_ids.json"))
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    error = next(validator.validate())
    assert error

    dataset = DatasetSchema.from_dict(schema_json("stadsdelen.json"))
    validator = PsqlIdentifierLengthValidator(dataset=dataset)
    with pytest.raises(StopIteration):
        # no validation error
        next(validator.validate())
