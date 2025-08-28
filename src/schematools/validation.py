"""Semantic JSON schema validation.

This package provides a simple :class:`Validator` class
that allows us to perform semantic validation
on a given JSON schema.

Semantic validation is different from structural validation,
as performed by the :mod:`jsonschema`,
in that it takes into account the implied logic behind a JSON schema.
For instance,
we might indicate that what identifies a table
is a set of properties (columns).
In Amsterdam Schema this is done using the ``identifier`` property
that can take an array of property names.
Structural validation cannot verify that the properties specified in that array
actually do exist in the schema.
Semantic validation can and should.

Semantic validations are created decorating a function with `_validator`.
"""

from __future__ import annotations

import operator
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial, wraps
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

from schematools import MAX_TABLE_NAME_LENGTH
from schematools.exceptions import SchemaObjectNotFound
from schematools.naming import to_snake_case, toCamelCase
from schematools.types import DatasetSchema, DatasetTableSchema, DatasetVersionSchema


@dataclass(frozen=True)
class ValidationError:
    """Capture validation errors."""

    validator_name: str
    message: str

    def __str__(self) -> str:
        return f"[{self.validator_name}] {self.message}"


_all: list[tuple[str, Callable[[DatasetSchema, str | None], Iterator[str]]]] = []


def run(dataset: DatasetSchema, location: str | None = None) -> Iterator[ValidationError]:
    r"""Run all registered validators.

    Yields:
        :class:`ValidationError`\s, if any.

    """  # noqa: W605
    for name, validator in _all:
        for msg in validator(dataset, location):
            yield ValidationError(validator_name=name, message=msg)


def _register_validator(name: str) -> Callable:
    """Marks a function as a validator and registers it with `run`.

    The function should yield strings describing the problem.
    `run` combines those strings with `name` into ValidationErrors.
    """
    if not name:
        raise ValueError("validator must have a name")

    def decorator(func: Callable[[DatasetSchema, str | None], Iterator[str]]) -> Callable:
        @wraps(func)
        def decorated(dataset: DatasetSchema, location: str | None = None) -> Iterator[str]:
            if func.__code__.co_argcount == 1:
                return func(dataset)
            else:
                return func(dataset, location)

        _all.append((name, decorated))
        return decorated

    return decorator


@_register_validator("camel case")
def _camelcase(dataset: DatasetSchema) -> Iterator[str]:
    """Checks that conversion to snake case and back leaves field identifiers unchanged."""
    for table in dataset.tables:
        for field in table.fields:
            error = _camelcase_ident(field.id)
            if error is not None:
                yield error


def _camelcase_ident(ident: str) -> str | None:
    if ident == "":
        return "empty identifier not allowed"
    camel = toCamelCase(to_snake_case(ident))
    if camel == ident:
        return None
    return f"{ident} does not survive conversion to snake case and back; suggestion: {camel}"


@_register_validator("enum type error")
def _enum_types(dataset: DatasetSchema, location: str | None) -> Iterator[str]:
    for table in dataset.tables:
        for field in table.fields:
            enum = field.get("enum")
            if not enum:
                continue

            if field.type == "integer":
                typ = int
                a = "an"
            elif field.type == "string":
                typ = str
                a = "a"
            else:
                yield f"{field.id}: enum of type {field.type} not possible"
                continue

            for v in enum:
                if not isinstance(v, typ):
                    yield f"value {v!r} in field {field.id} is not {a} {field.type}"


@_register_validator("ID does not match file path")
def _id_matches_path(dataset: DatasetSchema, location: str | None) -> Iterator[str]:
    """Dataset Identifiers should equal the parent paths to assure uniqueness.

    For datasets is subdirectories the path components should match the id like:
    'my/nested/location/dataset.json' -> 'myNestedLocation'

    Arguments:
        location: Location of the dataset file (relative to root or absolute)
    """
    if location is not None:
        path = Path(location)
        id_ = to_snake_case(dataset.id)

        # Ids are allowed to end with a number,
        # but the number should not be camelCased in the path.
        # So in this case the last instance of '_' is removed
        if id_.split("_")[-1].isdigit():
            id_ = "".join(id_.rsplit("_", 1))
        temp_path = path
        while len(id_):
            temp_path = temp_path.parent
            if not id_.endswith(temp_path.name):
                yield (
                    f"Id of the dataset {dataset.id} does not match "
                    f"the parent directory {path.parent}."
                )
                break
            id_ = id_[: -len(temp_path.name) - 1]


@_register_validator("Auth on identifier field")
def _id_auth(dataset: DatasetSchema) -> Iterator[str]:
    """Identifier fields should not have "auth" scopes.

    Handling these separately from table scopes is too much work for too little gain.
    """
    for table in dataset.tables:
        for ident in table.identifier:
            try:
                field = table.get_field_by_id(ident)
                if field.auth != {"OPENBAAR"}:
                    yield f"auth on field {ident!r} should go on the table instead"
            except SchemaObjectNotFound as e:
                yield f"{ident!r} listed in identifier list {table.identifier}, but: {e}"


@_register_validator("Identifier field with the wrong type")
def _id_type(dataset: DatasetSchema) -> Iterator[str]:
    """Identifier fields should have type integer or string."""
    for table in dataset.tables:
        for ident in table.identifier:
            try:
                field = table.get_field_by_id(ident)
                if field.type not in ["integer", "string"]:
                    yield (
                        f"identifier field {ident!r} should be a string or integer,"
                        f" is {field.type!r}"
                    )
            except SchemaObjectNotFound as e:
                yield f"{ident!r} listed in identifier list {table.identifier}, but: {e}"


@_register_validator("PostgreSQL identifier length")
def _postgres_identifier_length(dataset: DatasetSchema) -> Iterator[str]:
    """Validate inferred PostgreSQL table names for not exceeding max length.

    PostgreSQL has a maximum length for identifiers such as table names.
    We infer table names from dataset and table ids in the schemas.
    Those inferred table names should not exceed the max identifier length
    supported by PostgreSQL.
    """
    for table in dataset.get_tables(include_nested=True, include_through=True):
        db_name = table.db_name_variant(
            with_dataset_prefix=True, with_version=True, check_assert=False
        )
        # print(f"{db_name:>{MAX_TABLE_NAME_LENGTH}}")
        if (length := len(db_name)) > MAX_TABLE_NAME_LENGTH:
            excess = length - MAX_TABLE_NAME_LENGTH
            yield (
                f"Inferred PostgreSQL table name '{db_name}' is '{excess}' characters "
                f"too long. Maximum table name length is '{MAX_TABLE_NAME_LENGTH}'. Define "
                f"a `shortname`!"
            )


@_register_validator("PostgreSQL duplicate shortnames")
def _postgres_duplicate_shortnames(dataset: DatasetSchema) -> Iterator[str]:
    """
    A shortname can be set for fieldnames.
    These shortnames should however not be identical.
    """

    for table in dataset.get_tables(include_nested=True, include_through=True):
        shortnames = {}
        for field in table.get_fields():
            if field.has_shortname:
                if field.shortname in shortnames:
                    shortnames[field.shortname].append(field.id)
                else:
                    shortnames[field.shortname] = [field.id]

        for name, fields in shortnames.items():
            if len(fields) > 1:
                field_names = ",".join(fields)
                yield (f"Duplicate shortname '{name}' found for field: '{field_names}'")


@_register_validator("PostgreSQL duplicate abbreviated fieldnames")
def _postgres_duplicate_abbreviated_fieldnames(dataset: DatasetSchema) -> Iterator[str]:
    """Validate inferred PostgreSQL field names are not the same after being cut to 63 characters.

    PostgreSQL has a maximum length for identifiers such as field names.
    A shortname can be set such that identifiers are not exceeding this limit.
    Ensure that we do not have identical identifiers.
    """

    for table in dataset.get_tables(include_nested=True, include_through=True):
        fieldnames = {}
        for field in table.get_fields():
            if not field.has_shortname:
                field_name = field.id
                trimmed_name = field_name[:MAX_TABLE_NAME_LENGTH]
                if trimmed_name in fieldnames:
                    fieldnames[trimmed_name].append(field_name)
                else:
                    fieldnames[trimmed_name] = [field_name]

        for fields in fieldnames.values():
            if len(fields) > 1:
                names = "', '".join(fields)
                yield (f"Fields '{names}' share the same first 63 characters. Add a shortname.")


@_register_validator("repetitive identifiers")
def _repetitive_naming(dataset: DatasetSchema) -> Iterator[str]:
    """Identifier names should not repeat enclosing dataset/table names.

    This catches any dataset with a datasetThing table addressed by
    datasetThingIdentifier (should be dataset, thing, identifier).
    We make an exception for the case where dataset and table names are equal.
    """
    for table in dataset.tables:
        if table.id != dataset.id and table.id.startswith(dataset.id):
            yield f"table name {table.id!r} should not start with {dataset.id!r}"
        # NOTE: The code below is temporarily commented out because a lot of datasets are not
        # compliant with this rule (the precommit hook in ams-schema was
        # misconfigured, causing it to bypass checks on a lot of schemas for a long time).
        #
        # Making all datasets compliant with this is a lot of work and there is no known
        # component downstream which breaks because of violation of this rule, so until
        # we get all datasets compliant, we bypass this check.

        # for field in table.fields:
        # for prefix in [dataset.id, table.id]:
        # if field.id.startswith(prefix):
        # yield f"field name {field.id!r} should not start with {prefix!r}"


@_register_validator("identifier properties")
def _identifier_properties(dataset: DatasetSchema) -> Iterator[str]:
    """Validate that the identifier property refers to actual fields on the table definitions."""

    @dataclass
    class DerivedField:
        original: str
        derived: str

    for table in dataset.get_tables(include_nested=True):
        identifiers = set(table.identifier)
        table_fields = cast(set[str], set(map(operator.attrgetter("id"), table.fields)))
        if not identifiers.issubset(table_fields):
            missing_fields = identifiers - table_fields
            # The 'identifier' property is weird in that it is not exclusively defined in
            # terms of literally defined fields on the table. For instance, given a relation:
            #
            #     "indicatorDefinitie": {
            #       "type": "string",
            #       "relation": "bbga:indicatorenDefinities",
            #        "description": "De variabele in kwestie."
            #     }
            #
            # 'identifier' can refer to this field as 'identifierDefinitionId' (mind the
            # 'Id' postfix). Simply referring to this field (from 'identifier') as
            # 'indicatorDefinitie', eg as:
            #
            #     "identifier": ["indicatorDefinitie", "jaar", "gebiedcode15"],
            #
            #  will NOT work. It has to be postfixed with 'Id', eg:
            #
            #     "identifier": ["indicatorDefinitieId", "jaar", "gebiedcode15"],
            #
            # I think this is a bug is schema-tools, but for now I'll cover this case
            # explicitly.
            remove_id_suffix = cast(Callable[[str], str], partial(re.sub, r"(.+)Id", r"\1"))
            derived_fields = tuple(
                DerivedField(original=remove_id_suffix(f), derived=f) for f in missing_fields
            )
            for df in derived_fields:
                if df.original in table_fields:
                    missing_fields.discard(df.derived)
            if missing_fields:
                fields, have = ("fields", "have") if len(missing_fields) > 1 else ("field", "has")
                yield (
                    f"Property 'identifier' on table '{table.id}' refers to {fields} "
                    f"'{', '.join(missing_fields)}' that {have} not been defined on the "
                    "table."
                )


@_register_validator("mainGeometry")
def _check_maingeometry(dataset: DatasetSchema) -> Iterator[str]:
    for table in dataset.tables:
        # We can't use table.main_geometry here, because it has a default value
        # "geometry". We can't rely on that always existing.
        main_geo = table["schema"].get("mainGeometry")
        if main_geo is None:
            # mainGeometry should exist if a geometry field exists
            # but none of the geometry fields is called "geometry"
            if table.has_geometry_fields and not any(
                field.is_geo and field.id == "geometry" for field in table.fields
            ):
                yield (
                    f"'mainGeometry' is required but not defined in table ${table.id}."
                    "This table has fields of type geometry,"
                    "but none of these fields is called 'geometry'."
                )
            continue

        try:
            field = table.get_field_by_id(main_geo)
            if not field.is_geo:
                yield f"mainGeometry = {field.id!r} is not a geometry field, type = {field.type!r}"
        except SchemaObjectNotFound as e:
            yield f"mainGeometry = {main_geo!r}, but: {e}"


@_register_validator("crs")
def _check_crs(dataset: DatasetSchema) -> Iterator[str]:
    """Check that a valid crs exists for each geometry field."""
    if dataset.data.get("crs") is None:
        for table in dataset.tables:
            if table.data.get("crs") is None:
                for field in table.fields:
                    if field.is_geo and field.crs is None:
                        yield (
                            f"No coordinate reference system defined for field {field.name}. "
                            "A crs property should exist on the field or its parent table "
                            'or parent dataset. suggestion: "EPSG:28992".'
                        )


@_register_validator("display")
def _check_display(dataset: DatasetSchema) -> Iterator[str]:
    for table in dataset.tables:
        display_field_id = table["schema"].get("display")
        if display_field_id is None:
            continue

        try:
            field = table.get_field_by_id(display_field_id)
            if field.auth != {"OPENBAAR"}:
                yield (
                    f"'auth' property on the display field: {display_field_id!r} is not allowed. "
                    " Display fields can not have an 'auth' property."
                )
        except SchemaObjectNotFound as e:
            yield f"display = {display_field_id!r}, but: {e}"


@_register_validator("property formats")
def _property_formats(dataset: DatasetSchema) -> Iterator[str]:
    """Properties should have a valid "format", or none at all."""
    # TODO Should we be validating these in the meta-schema instead of here?
    ALLOWED = {
        # Default value for DatasetFieldSchema.format.
        None,
        # Listed in the schema spec.
        "date",
        "date-time",
        "duration",
        "email",
        "hostname",
        "idn-email",
        "idn-hostname",
        "ipv4",
        "ipv6",
        "iri",
        "iri-reference",
        "time",
        "uri",
        "uri-reference",
        # XXX In actual use, not sure what it's supposed to mean.
        "summary",
    }

    for table in dataset.tables:
        for field in table.fields:
            if field.type == "str" and field.format not in ALLOWED:
                yield f"Format {field.format!r} not allowed, must be one of {ALLOWED!r}"


@_register_validator("auth across relations")
def _relation_auth(dataset: DatasetSchema) -> Iterator[str]:
    """Relation fields should have at least the auth scopes of the field they refer to."""
    for table in dataset.tables:
        for field in table.get_fields(include_subfields=True):
            our_auth = table.dataset.auth | table.auth | field.auth

            rel_table = field.related_table
            if not rel_table:
                continue

            if (
                not our_auth.issuperset(rel_table.dataset.auth)
                or not our_auth.issuperset(rel_table.auth)
                or not all(
                    our_auth.issuperset(rel_table.get_field_by_id(f).auth)
                    for f in (field.related_field_ids or [])
                )
            ):
                scopes = set(rel_table.dataset.auth) | set(rel_table.auth)
                for f in field.related_field_ids:
                    scopes |= rel_table.get_field_by_id(f).auth
                scopes.remove("OPENBAAR")  # Not very interesting.
                yield f"{table.id}.{field.id} requires scopes {sorted(scopes)}"


@_register_validator("reasons non public exists")
def _reasons_non_public_exists(dataset: DatasetSchema) -> Iterator[str]:
    """A ReasonsNonPublic field should be present on the highest non-public scope.

    For non-public fields in a public table, reasonsNonPublic can be set on table
    level. This is less verbose when all the non-public fields have the same
    reason for being non-public.

    """
    if dataset.auth == {"OPENBAAR"}:
        for table in dataset.tables:
            if table.data.get("reasonsNonPublic") is None:
                if table.auth == {"OPENBAAR"}:
                    for field in table.fields:
                        if (
                            field.auth != {"OPENBAAR"}
                            and field.data.get("reasonsNonPublic") is None
                        ):
                            yield (
                                f"Non-public field {field.id} or it's parent table "
                                "should have a 'reasonsNonPublic' property."
                            )
                else:
                    yield (
                        f"Non-public table {table.id} should have a 'reasonsNonPublic' property."
                    )
    elif dataset.data.get("reasonsNonPublic") is None:
        yield (f"Non-public dataset {dataset.id} should have a 'reasonsNonPublic' property.")


@_register_validator("reasons non public value")
def _reasons_non_public_value(dataset: DatasetSchema) -> Iterator[str]:
    """A reasonsNonPublic field in a published dataset should not contain a placeholder."""
    placeholder_value = "nader te bepalen"
    versions = dataset.versions.values()
    any_version_available = any(
        version.status == DatasetSchema.Status.beschikbaar for version in versions
    )
    if not any_version_available:
        return
    if placeholder_value in dataset.data.get("reasonsNonPublic", []):
        yield (
            f"Placeholder value '{placeholder_value}' not allowed in "
            f"ReasonsNonPublic property of dataset {dataset.id}."
        )
    for table in dataset.tables:
        if placeholder_value in table.data.get("reasonsNonPublic", []):
            yield (
                f"Placeholder value '{placeholder_value}' not allowed "
                f"ReasonsNonPublic property of table {table.id}."
            )
        for field in table.fields:
            if placeholder_value in field.data.get("reasonsNonPublic", []):
                yield (
                    f"Placeholder value '{placeholder_value}' not allowed "
                    f"ReasonsNonPublic property of field {field.id}."
                )


@_register_validator("schema ref")
def _check_schema_ref(dataset: DatasetSchema) -> Iterator[str]:
    """Check that $ref field for all tables has correct hostname."""
    for table in dataset.tables:
        fragments = urlparse(table["schema"]["properties"]["schema"]["$ref"])
        if fragments.hostname != "schemas.data.amsterdam.nl" or fragments.scheme != "https":
            yield (
                f"Incorrect `$ref` for {table.id}. Value should be "
                f"`https://schemas.data.amsterdam.nl`"
            )


@_register_validator("defaultVersion")
def _check_default_version(dataset: DatasetSchema) -> Iterator[str]:
    """Check that defaultVersion field for a dataset matches the only 'beschikbaar' version."""
    enabled_versions = []
    for version_number, version in dataset.versions.items():
        if version.status == DatasetSchema.Status.beschikbaar:
            enabled_versions.append(version_number)
            if version_number != dataset.default_version:
                yield (
                    f"Default version {dataset.default_version} does not match enabled "
                    f"version {version_number}"
                )


@_register_validator("production version tables")
def _check_production_version_tables(dataset: DatasetSchema) -> Iterator[str]:
    """Check that a production version (>= v1) contains no v0 tables."""
    non_production_versions = ["v0"]
    version_regex = r"(v?(?P<major>\d+)(?:\.(?P<minor>\d+)(?:\.(?P<patch>\d+))?)?)"
    for version_number, version in dataset.versions.items():
        if version_number in non_production_versions:
            continue

        for table in version.data.get("tables"):
            # Don't validate inline tables, to allow backwards compatibility
            if "$ref" in table:
                table_version = re.search(version_regex, table["$ref"]).group(0)
                if table_version in non_production_versions:
                    yield (
                        f"Dataset version ({version_number}) cannot contain non-production "
                        f"table [{table['$ref']}]"
                    )


@_register_validator("stable version experimental tables")
def _check_production_version_experimental_tables(dataset: DatasetSchema) -> Iterator[str]:
    """Check that a stable dataset version contains no experimental tables."""
    for version_number, version in dataset.versions.items():
        if version.lifecycle_status == DatasetVersionSchema.LifecycleStatus.experimental:
            continue

        for table in version.tables:
            # Don't validate inline tables, to allow backwards compatibility
            if table.lifecycle_status == DatasetTableSchema.LifecycleStatus.experimental:
                yield (
                    f"Stable dataset {dataset.id} ({version_number}) cannot have tables with "
                    f"lifecycleStatus of 'experimental'."
                )


@_register_validator("lifecycle status")
def _check_lifecycle_status(dataset: DatasetSchema) -> Iterator[str]:
    """
    Check that a non production version (< v1) contains can't have a
    lifecycle status of 'stable'.
    """
    non_production_versions = ["v0"]
    for version_number, version in dataset.versions.items():
        if (
            version_number in non_production_versions
            and version.lifecycle_status == DatasetVersionSchema.LifecycleStatus.stable
        ):
            yield (
                f"Dataset version ({version_number}) cannot have a lifecycleStatus of "
                f"'stable' while being a non-production version."
            )


def validate_dataset(
    previous_tables: list[dict[str, str]],
    current_tables: list[dict[str, str]],
) -> list[str]:
    """Validates that the current version of a stable dataset does not introduce breaking changes.

    We check:
    1. wheter an existing tables is removed
    2. whether an existing table is changed to another version

    NB. This is not a registered validator that has been registered, as here we have to
    compare tables from two versions of the same dataset.
    """
    dataset_errors = []
    for previous_table in previous_tables:
        if previous_table["id"] not in [t["id"] for t in current_tables]:
            dataset_errors.append(f"Table {previous_table['id']} has been removed.")
            continue

        current_table = next(
            (table for table in current_tables if table["id"] == previous_table["id"]), None
        )
        if previous_table["$ref"] != current_table["$ref"]:
            dataset_errors.append(
                f"Table {previous_table['id']} has changed version. Previous version: "
                f"{previous_table['$ref']}, current version: {current_table['$ref']}."
            )

    return dataset_errors


PROPERTIES_INTRODUCING_BREAKING_CHANGES = ["type", "$ref", "format", "relation", "enum"]
IGNORED_FIELDS = ["schema"]  # This is not a database column.


def validate_table(
    previous_fields: dict,
    current_fields: dict,
    parent_field: str | None = None,
    object_path: str | None = None,
) -> list[str]:
    """Validates that the current version of the table does not introduce breaking changes.

    We check:
    1. whether a field has been deleted
    2. whether any of the properties in `PROPERTIES_INTRODUCING_BREAKING_CHANGES` has changed

    This works recursively for array items and object properties.

    NB. This is not a registered validator that has been registered, as here we have to
    compare fields from two versions of the same table.
    """
    table_errors = []
    current_object_path = object_path
    for previous_field_name, previous_field in previous_fields.items():
        if previous_field_name in IGNORED_FIELDS:
            continue

        column_name = parent_field or previous_field_name

        # check if field is deleted
        if previous_field_name not in current_fields:
            if parent_field:
                table_errors.append(
                    f"Property {object_path}.{previous_field_name} would be "
                    f"deleted from column {column_name}."
                )
            else:
                table_errors.append(f"Column {column_name} would be deleted.")
            continue

        current_field = current_fields[previous_field_name]

        # check breaking changes in field properties
        for prop in PROPERTIES_INTRODUCING_BREAKING_CHANGES:
            if previous_field.get(prop) != current_field.get(prop):
                if parent_field:
                    current_object_path = (
                        f"{object_path}.{previous_field_name}"
                        if object_path
                        else previous_field_name
                    )
                prop_name = f"{current_object_path}.{prop}" if current_object_path else prop
                table_errors.append(f"Column {column_name} would change {prop_name}.")

        # recursively check array items
        if previous_field.get("type") == "array":
            previous_array_items = {"items": previous_field.get("items")}
            current_array_items = {"items": current_field.get("items")}
            table_errors.extend(
                validate_table(previous_array_items, current_array_items, column_name)
            )

        # recursively check object properties
        if previous_field.get("type") == "object":
            previous_object_properties = previous_field.get("properties")
            current_object_properties = current_field.get("properties")
            if previous_field_name != column_name:
                object_path = (
                    previous_field_name
                    if object_path is None
                    else f"{object_path}.{previous_field_name}"
                )

            table_errors.extend(
                validate_table(
                    previous_object_properties,
                    current_object_properties,
                    column_name,
                    object_path,
                )
            )

    return table_errors
