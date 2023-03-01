from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from schematools import validation
from schematools.permissions import PUBLIC_SCOPE
from schematools.types import DatasetSchema
from schematools.validation import (
    _active_versions,
    _check_display,
    _check_maingeometry,
    _identifier_properties,
)


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


def test_enum_types(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("enum_types.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error.validator_name == "enum type error"
    assert error.message == "value 'foo' in field enumInts is not an integer"

    error = next(errors)
    assert error.validator_name == "enum type error"
    assert error.message == "value 2 in field enumStrs is not a string"

    error = next(errors)
    assert error.validator_name == "enum type error"
    assert error.message == "enumFloats: enum of type number not possible"

    assert list(errors) == []


def test_id_auth(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("id_auth.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error
    assert error.validator_name == "Auth on identifier field"
    assert """auth on field 'id'""" in error.message

    assert list(errors) == []


def test_id_type(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("id_type.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error
    assert error.validator_name == "Identifier field with the wrong type"
    assert """field 'uniqid'""" in error.message
    assert """'number'""" in error.message

    assert list(errors) == []


def test_id_matches_path(here: Path, schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("stadsdelen.json")

    # No errors when id equals parent path name
    errors = validation.run(dataset, str(here / "files/datasets/stadsdelen/dataset.json"))
    assert list(errors) == []

    # Error when not equal
    errors = validation.run(dataset, str(here / "files/datasets/regios/dataset.json"))
    error = next(errors)
    assert error
    assert error.validator_name == "ID does not match file path"
    assert re.match(
        r"^Id of the dataset stadsdelen does not match the parent directory"
        r" .*/files/datasets/regios\.$",
        error.message,
    )
    assert list(errors) == []

    # Test datasets in sub directory
    dataset.__setitem__("id", "beheerkaartCbsGrid")
    errors = validation.run(dataset, str(here / "files/datasets/bierkaart/cbs_grid/dataset.json"))
    error = next(errors)
    assert error
    assert error.validator_name == "ID does not match file path"
    assert re.match(
        r"^Id of the dataset beheerkaartCbsGrid does not match the parent directory"
        r" .*/files/datasets/bierkaart/cbs_grid\.$",
        error.message,
    )
    assert list(errors) == []

    errors = validation.run(
        dataset, str(here / "files/datasets/beheerkaart/cbs_grid/dataset.json")
    )
    assert list(errors) == []

    # Test identifiers ending with a number
    dataset.__setitem__("id", "covid19")
    errors = validation.run(dataset, str(here / "files/datasets/covid19/dataset.json"))
    assert list(errors) == []


def test_crs(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("crs_validation.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error
    assert error.validator_name == "crs"
    assert """No coordinate reference system defined for field""" in error.message

    assert list(errors) == []


def test_postgres_identifier_length(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("long_ids.json")

    error = next(validation.run(dataset))
    assert error
    assert error.validator_name == "PostgreSQL identifier length"
    assert "absurdly_long" in error.message

    dataset = schema_loader.get_dataset_from_file("stadsdelen.json")
    assert list(validation.run(dataset)) == []  # no validation errors


def test_identifier_properties(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("identifier_ref.json")
    error = next(validation.run(dataset))
    assert error
    assert "foobar" in error.message

    dataset = schema_loader.get_dataset_from_file("stadsdelen.json")
    assert list(_identifier_properties(dataset)) == []  # no validation errors


def test_active_versions(schema_loader) -> None:
    dataset = schema_loader.get_dataset("gebieden_sep_tables")
    table_versions = dataset.table_versions["bouwblokken"]
    table_versions.id = "BOUWBLOKKEN"
    error = next(validation.run(dataset))
    assert error
    assert "does not match with id" in error.message

    dataset = schema_loader.get_dataset("gebieden_sep_tables")
    dataset["tables"][0]["activeVersions"] = {"9.8.1": "bouwblokken/v1.0.0"}
    error = next(validation.run(dataset))
    assert error
    assert "does not match with version" in error.message

    dataset = schema_loader.get_dataset("gebieden_sep_tables")
    assert list(_active_versions(dataset)) == []  # no validation errors


def test_main_geometry(schema_loader, gebieden_schema) -> None:
    dataset = schema_loader.get_dataset_from_file("meetbouten.json")
    assert list(_check_maingeometry(dataset)) == []

    dataset.get_table_by_id("meetbouten")["schema"]["mainGeometry"] = None
    error = next(validation.run(dataset))
    assert "'mainGeometry' is required but not defined in table" in error.message

    dataset.get_table_by_id("meetbouten")["schema"]["mainGeometry"] = "not_a_geometry"
    error = next(validation.run(dataset))
    assert "mainGeometry = 'not_a_geometry'" in error.message
    assert "Field 'not_a_geometry' does not exist" in error.message

    dataset.get_table_by_id("meetbouten")["schema"]["mainGeometry"] = "merkOmschrijving"
    error = next(validation.run(dataset))
    assert error.message == (
        "mainGeometry = 'merkOmschrijving' is not a geometry field, type = 'string'"
    )


def test_display(here: Path, schema_loader) -> None:
    schema_loader.get_dataset_from_file("gebieden.json")  # fill cache
    dataset = schema_loader.get_dataset_from_file("meetbouten.json")
    assert list(_check_display(dataset)) == []

    table = dataset.get_table_by_id("meetbouten")
    table["schema"]["display"] = "not_a_field"
    error = next(validation.run(dataset))
    assert "display = 'not_a_field'" in error.message
    assert "Field 'not_a_field' does not exist" in error.message

    table["schema"]["display"] = "merkCode"
    table["schema"]["properties"]["merkCode"]["auth"] = "some_scope"
    table.__dict__.pop("fields", None)  # clear cached property
    error = next(validation.run(dataset))
    assert "'auth' property on the display field: 'merkCode' is not allowed." in error.message


def test_rel_auth_dataset(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("rel_auth.json")
    dataset["auth"] = ["HAMMERTIME"]
    dataset["reasonsNonPublic"] = ["U can't touch this"]

    errors = list(validation.run(dataset))
    assert errors == []


def test_rel_auth_dataset_public(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("rel_auth.json")
    dataset["auth"] = [PUBLIC_SCOPE]

    errors = list(validation.run(dataset))
    assert len(errors) == 0, errors


def test_rel_auth_table(here: Path) -> None:
    dataset_json = json.load(open(here / "files/datasets/rel_auth.json"))
    table = next(t for t in dataset_json["tables"] if t["id"] == "base")
    table["auth"] = ["HAMMERTIME"]
    table["reasonsNonPublic"] = ["U can't touch this"]
    dataset = DatasetSchema.from_dict(dataset_json)

    errors = list(validation.run(dataset))
    assert len(errors) == 1, errors
    assert "requires scopes ['HAMMERTIME']" in str(errors[0])


def test_rel_auth_field(here: Path) -> None:
    dataset_json = json.load(open(here / "files/datasets/rel_auth.json"))
    table = next(t for t in dataset_json["tables"] if t["id"] == "base")
    field = table["schema"]["properties"]["stop"]
    field["auth"] = ["HAMMERTIME"]

    dataset = DatasetSchema.from_dict(dataset_json)
    errors = list(validation.run(dataset))

    assert len(errors) >= 1, errors
    assert any("requires scopes ['HAMMERTIME']" in str(e) for e in errors)


@pytest.mark.skip(reason="See comment in validator function")
def test_repetitive_naming(here: Path, schema_loader) -> None:
    dataset = schema_loader.get_dataset("repetitive")
    errors = {str(e) for e in validation.run(dataset)}

    assert errors == {
        "[repetitive identifiers] " + e
        for e in [
            "table name 'repetitiveTable' should not start with 'repetitive'",
            "field name 'repetitiveTableField' should not start with 'repetitive'",
            "field name 'repetitiveTableField' should not start with 'repetitiveTable'",
        ]
    }


def test_reasons_non_public_exists(here: Path, schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("hr_auth.json")
    errors = list(validation.run(dataset))

    # Test an error is given for the highest non-public scope
    # and only for the highest non-public scope.
    assert len(errors) == 1
    assert errors[0].message == "Non-public dataset hr should have a 'reasonsNonPublic' property."

    dataset["auth"] = [PUBLIC_SCOPE]
    errors = list(validation.run(dataset))
    assert len(errors) == 1
    assert (
        errors[0].message
        == "Non-public table sbiactiviteiten should have a 'reasonsNonPublic' property."
    )

    # Test no error is given when a reason is present
    dataset_json = json.load((here / "files/datasets/hr_auth.json").open())
    dataset_json["tables"][0]["reasonsNonPublic"] = ["5.1 1c: Bevat persoonsgegevens"]
    dataset = DatasetSchema.from_dict(dataset_json)
    dataset["auth"] = [PUBLIC_SCOPE]
    errors = list(validation.run(dataset))
    assert len(errors) == 0


def test_reasons_non_public_value(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("hr_auth.json")
    dataset["reasonsNonPublic"] = ["5.1 1c: Bevat persoonsgegevens", "nader te bepalen"]
    errors = list(validation.run(dataset))

    # Test an error is given for the placeholder value in a dataset with status = beschikbaar.
    assert len(errors) == 1
    assert "not allowed in ReasonsNonPublic property of dataset hr." in errors[0].message

    # Test no error is given for the placeholder value in a dataset with status != beschikbaar.
    dataset["status"] = "niet_beschikbaar"
    errors = list(validation.run(dataset))
    assert len(errors) == 0

    # Test no error is given for other values of reasonsNonPublic
    dataset["status"] = "beschikbaar"
    dataset["reasonsNonPublic"] = ["5.1 1c: Bevat persoonsgegevens"]
    errors = list(validation.run(dataset))
    assert len(errors) == 0


def test_schema_ref(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("schema_ref_validation.json")

    errors = validation.run(dataset)

    error = next(errors)
    assert error
    assert error.validator_name == "schema ref"
    assert """Incorrect `$ref` for""" in error.message

    assert list(errors) == []
