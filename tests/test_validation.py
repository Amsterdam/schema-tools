from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from schematools import validation
from schematools.permissions import PUBLIC_SCOPE
from schematools.types import DatasetSchema
from schematools.validation import (
    PROPERTIES_INTRODUCING_BREAKING_CHANGES,
    _check_display,
    _check_maingeometry,
    _identifier_properties,
    validate_dataset,
    validate_table,
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


def test_postgres_duplicate_shortnames(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("duplicate_shortnames.json")

    error = next(validation.run(dataset))
    assert error
    assert error.validator_name == "PostgreSQL duplicate shortnames"
    assert error.message == "Duplicate shortname 'sameName' found for field: 'veld1,veld2'"


def test_postgres_duplicate_abbreviated_fieldnames(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("abbreviated_fieldnames.json")

    error = next(validation.run(dataset))
    assert error
    assert error.validator_name == "PostgreSQL duplicate abbreviated fieldnames"
    assert (
        error.message
        == "Fields 'eenVestigingIsGebouwOfEenComplexGebouwenDuurzameUitoefeningActiviteitenOndernemingRechtspersoon',"
        " 'eenVestigingIsGebouwOfEenComplexGebouwenDuurzameUitoefeningActiviteitenOndernemingRechtspersoon2' share "
        "the same first 63 characters. Add a shortname."
    )


def test_postgres_duplicate_abbreviated_fieldnames_with_shortname(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("abbreviated_fieldnames_with_shortname.json")
    assert list(validation.run(dataset)) == []  # no validation errors


def test_identifier_properties(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("identifier_ref.json")
    error = next(validation.run(dataset))
    assert error
    assert "foobar" in error.message

    dataset = schema_loader.get_dataset_from_file("stadsdelen.json")
    assert list(_identifier_properties(dataset)) == []  # no validation errors


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
    with (here / "files/datasets/rel_auth.json").open() as f:
        dataset_json = json.load(f)
    table = next(t for t in dataset_json["versions"]["v1"]["tables"] if t["id"] == "base")
    table["auth"] = ["HAMMERTIME"]
    table["reasonsNonPublic"] = ["U can't touch this"]
    dataset = DatasetSchema.from_dict(dataset_json)

    errors = list(validation.run(dataset))
    assert len(errors) == 1, errors
    assert "requires scopes ['HAMMERTIME']" in str(errors[0])


def test_rel_auth_field(here: Path) -> None:
    with (here / "files/datasets/rel_auth.json").open() as f:
        dataset_json = json.load(f)
    table = next(t for t in dataset_json["versions"]["v1"]["tables"] if t["id"] == "base")
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
    dataset_json["versions"]["v1"]["tables"][0]["reasonsNonPublic"] = [
        "5.1 1c: Bevat persoonsgegevens"
    ]
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
    dataset.versions["v1"]["status"] = "niet_beschikbaar"
    errors = list(validation.run(dataset))
    assert len(errors) == 0

    # Test no error is given for other values of reasonsNonPublic
    dataset.versions["v1"]["status"] = "beschikbaar"
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


def test_check_default_version(schema_loader) -> None:
    dataset = schema_loader.get_dataset_from_file("schema_default_version.json")

    errors = list(validation.run(dataset))
    assert len(errors) == 0

    # Prove that wrong default version gives an error
    dataset["defaultVersion"] = "v2"
    errors = list(validation.run(dataset))
    assert len(errors) == 1
    assert "Default version v2 does not match enabled version v1" in errors[0].message


def test_production_version_tables(schema_loader) -> None:
    dataset = schema_loader.get_dataset("production_version")

    errors = list(validation.run(dataset))
    assert len(errors) == 1
    assert (
        "Dataset version (v1) cannot contain non-production table [tables/v0]" in errors[0].message
    )


def test_production_version_experimental_tables(schema_loader) -> None:
    dataset = schema_loader.get_dataset("experimental_tables")

    errors = list(validation.run(dataset))
    assert len(errors) == 1
    assert (
        "Stable dataset experimental_tables (v1) cannot have tables with lifecycleStatus of 'experimental'."
        in errors[0].message
    )


def test_check_lifecycle_status(schema_loader) -> None:
    dataset = schema_loader.get_dataset("lifecycle_status")

    errors = list(validation.run(dataset))
    assert len(errors) == 1
    assert (
        "Dataset version (v0) cannot have a lifecycleStatus of 'stable' while being a non-production version."
        in errors[0].message
    )


@pytest.mark.parametrize(
    "prev,curr,errors",
    [
        # No changes
        ([{"id": "table", "$ref": "table/v1"}], [{"id": "table", "$ref": "table/v1"}], []),
        # Added table
        (
            [{"id": "table", "$ref": "table/v1"}],
            [{"id": "table", "$ref": "table/v1"}, {"id": "table2", "$ref": "table2/v1"}],
            [],
        ),
        # Removed table
        (
            [{"id": "table", "$ref": "table/v1"}, {"id": "table2", "$ref": "table2/v1"}],
            [{"id": "table", "$ref": "table/v1"}],
            ["Table table2 has been removed."],
        ),
        # Changed table version
        (
            [{"id": "table", "$ref": "table/v1"}],
            [{"id": "table", "$ref": "table/v2"}],
            [
                "Table table has changed version. Previous version: table/v1, current version: table/v2."
            ],
        ),
        # Multiple errors
        (
            [{"id": "table", "$ref": "table/v1"}, {"id": "table2", "$ref": "table2/v1"}],
            [{"id": "table", "$ref": "table/v2"}],
            [
                "Table table has changed version. Previous version: table/v1, current version: table/v2.",
                "Table table2 has been removed.",
            ],
        ),
    ],
)
def test_validate_dataset(prev, curr, errors):
    table_errors = validate_dataset(prev, curr)
    assert table_errors == errors


@pytest.mark.parametrize(
    "prev,curr,errors",
    [
        # No changes
        ({"name": {"type": "string"}}, {"name": {"type": "string"}}, []),
        # Deleted field
        ({"name": {"type": "string"}}, {}, ["Column name would be deleted."]),
        # Changed schema ref
        (
            {
                "schema": {
                    "$ref": "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema"
                }
            },
            {
                "schema": {
                    "$ref": "https://schemas.data.amsterdam.nl/schema@v3.1.0#/definitions/schema"
                }
            },
            [],
        ),
        # Changed array item type
        (
            {"list": {"type": "array", "items": {"type": "string"}}},
            {"list": {"type": "array", "items": {"type": "integer"}}},
            ["Column list would change items.type."],
        ),
        # Changed object property.
        (
            {"object": {"type": "object", "properties": {"element": {"type": "string"}}}},
            {"object": {"type": "object", "properties": {"element": {"type": "integer"}}}},
            ["Column object would change element.type."],
        ),
        # Changed object property type within an array
        (
            {
                "list": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"element": {"type": "string"}}},
                }
            },
            {
                "list": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"element": {"type": "integer"}}},
                }
            },
            ["Column list would change items.element.type."],
        ),
        # Changed subproperty of a property of an object
        (
            {
                "object": {
                    "type": "object",
                    "properties": {
                        "element": {
                            "type": "object",
                            "properties": {"subelement": {"type": "string"}},
                        }
                    },
                }
            },
            {
                "object": {
                    "type": "object",
                    "properties": {
                        "element": {
                            "type": "object",
                            "properties": {"subelement": {"type": "integer"}},
                        }
                    },
                }
            },
            ["Column object would change element.subelement.type."],
        ),
        # Multiple breaking changes
        (
            {
                "object": {
                    "type": "object",
                    "properties": {
                        "element": {
                            "type": "object",
                            "properties": {
                                "subelement": {"type": "string"},
                                "subelement2": {"type": "string"},
                            },
                        }
                    },
                    "description": "Original description",
                    "relation": "table1:object_1",
                },
                "id": {"type": "string"},
                "deprecated_field": {"type": "string"},
            },
            {
                "object": {
                    "type": "object",
                    "properties": {
                        "element": {
                            "type": "object",
                            "properties": {"subelement": {"type": "integer"}},
                        }
                    },
                    "description": "Description 2.0",
                    "relation": "table2:object_2",
                },
                "id": {"type": "integer"},
            },
            [
                "Column object would change relation.",
                "Column object would change element.subelement.type.",
                "Property element.subelement2 would be deleted from column object.",
                "Column id would change type.",
                "Column deprecated_field would be deleted.",
            ],
        ),
    ]
    + [
        # All properties that should stay the same.
        (
            {"property": {prop: "string"}},
            {"property": {prop: "integer"}},
            [f"Column property would change {prop}."],
        )
        for prop in PROPERTIES_INTRODUCING_BREAKING_CHANGES
    ],
)
def test_validate_table(prev, curr, errors):
    table_errors = validate_table(prev, curr)
    assert table_errors == errors
