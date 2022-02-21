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
from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Iterator, List, Optional, Set, cast

from schematools import MAX_TABLE_NAME_LENGTH
from schematools.exceptions import SchemaObjectNotFound
from schematools.types import DatasetSchema, SemVer, TableVersions
from schematools.utils import to_snake_case, toCamelCase


@dataclass(frozen=True)
class ValidationError:
    """Capture validation errors."""

    validator_name: str
    message: str

    def __str__(self) -> str:
        return f"[{self.validator_name}] {self.message}"


class ValidationException(Exception):
    """Raised when validation fails to execute.

    .. note::

       This is not for validation errors. See :class:`ValidationError` instead.
    """

    message: str

    def __init__(self, message: str) -> None:  # noqa: D107
        super().__init__(message)
        self.message = message


_all: List[tuple[str, Callable[[DatasetSchema], Iterator[str]]]] = []


def run(dataset: DatasetSchema) -> Iterator[ValidationError]:
    r"""Run all registered validators.

    Yields:
        :class:`ValidationError`\s, if any.

    """  # noqa: W605
    for name, validator in _all:
        for msg in validator(dataset):
            yield ValidationError(validator_name=name, message=msg)


def _register_validator(name: str) -> Callable:
    """Marks a function as a validator and registers it with `run`.

    The function should yield strings describing the problem.
    `run` combines those strings with `name` into ValidationErrors.
    """
    if not name:
        raise ValueError("validator must have a name")

    def decorator(func: Callable[[DatasetSchema], Iterator[str]]) -> Callable:
        @wraps(func)
        def decorated(dataset: DatasetSchema) -> Iterator[str]:
            return func(dataset)

        _all.append((name, decorated))
        return decorated

    return decorator


@_register_validator("camel case")
def _camelcase(dataset: DatasetSchema) -> Iterator[str]:
    """Checks that conversion to snake case and back leaves field identifiers unchanged."""
    for table in dataset.tables:
        for field in table.get_fields(include_subfields=False):
            error = _camelcase_ident(field.id)
            if error is not None:
                yield error


def _camelcase_ident(ident: str) -> Optional[str]:
    if ident == "":
        return "empty identifier not allowed"
    camel = toCamelCase(to_snake_case(ident))
    if camel == ident:
        return None
    return f"{ident} does not survive conversion to snake case and back; suggestion: {camel}"


@_register_validator("Auth on identifier field")
def _id_auth(dataset: DatasetSchema) -> Iterator[str]:
    """Identifier fields should not have "auth" scopes.

    Handling these separately from table scopes is too much work for too little gain.
    """
    for table in dataset.tables:
        for ident in table.identifier:
            try:
                field = table.get_field_by_id(ident)
                if field.auth:
                    yield f"auth on field {ident!r} should go on the table instead"
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
        db_name = table.db_name(with_dataset_prefix=True, with_version=True, check_assert=False)
        # print(f"{db_name:>{MAX_TABLE_NAME_LENGTH}}")
        if (length := len(db_name)) > MAX_TABLE_NAME_LENGTH:
            excess = length - MAX_TABLE_NAME_LENGTH
            yield (
                f"Inferred PostgreSQL table name '{db_name}' is '{excess}' characters "
                f"too long. Maximum table name length is '{MAX_TABLE_NAME_LENGTH}'. Define "
                f"a `shortname`!"
            )


@_register_validator("identifier properties")
def _identifier_properties(dataset: DatasetSchema) -> Iterator[str]:
    """Validate that the identifier property refers to actual fields on the table definitions."""

    @dataclass
    class DerivedField:
        original: str
        derived: str

    for table in dataset.get_tables(include_nested=True):
        identifiers = set(table.identifier)
        table_fields = cast(Set[str], set(map(operator.attrgetter("id"), table.fields)))
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
                map(
                    lambda f: DerivedField(original=remove_id_suffix(f), derived=f),
                    missing_fields,
                )
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


@_register_validator("active versions")
def _active_versions(dataset: DatasetSchema) -> Iterator[str]:
    """Validate activeVersions and table identifiers in referenced tables."""
    # The current Amsterdam Meta Schema does not allow for inline definitions of multiple
    # active tables versions. In addition :class:`DatasetSchema`'s
    # :property:`~DatasetSchema.tables` property and :meth:~DatasetSchema.get_tables` method
    # still assume that there will always be one and only one version. The part of
    # :class:`DatasetSchema` that has gained some knowledge of multiple active versions is
    # its internal representation with the addition of the :class:`TableVersions` class.
    # Hence it is the internal representation that we use for this validation.
    #
    # This obviously is a stop gap. Ideally we have a more, arguably, sensible definition of
    # multiple active version in the Amsterdam Meta Schema (eg an inline definition). When
    # we do, we are in a position to restructure our abstraction (eg :class:`DatasetSchema`,
    # etc) more definitely. And as a result can rely on those abstractions for our
    # validation instead of some internal representation.
    for tv in dataset["tables"]:
        assert isinstance(  # noqa: S101
            tv, TableVersions
        ), 'Someone messed with the internal representation of DatasetSchema["tables"].'
        for version, table in tv.active.items():
            assert isinstance(table, dict)  # noqa: S101
            if tv.id != table["id"]:
                yield (
                    f"Table {tv.id!r} with version number '{version}' does not match with "
                    f"id {table['id']!r} of the referenced table."
                )
            version_in_table = SemVer(table["version"])
            if version != version_in_table:
                yield (
                    f"Version number '{version}' in activeVersions for table {table['id']!r} "
                    f"does not match with version number '{version_in_table}' of the "
                    "referenced table."
                )


@_register_validator("mainGeometry")
def _check_maingeometry(dataset: DatasetSchema) -> Iterator[str]:
    for table in dataset.tables:
        # We can't use table.main_geometry here, because it has a default value
        # "geometry". We can't rely on that always existing.
        main_geo = table["schema"].get("mainGeometry")
        if main_geo is None:
            continue

        try:
            field = table.get_field_by_id(main_geo)
            if not field.is_geo:
                yield f"mainGeometry = {field.id!r} is not a geometry field, type = {field.type!r}"
        except SchemaObjectNotFound as e:
            yield str(e)


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
        for field in table.get_fields():
            if field.type == "str" and field.format not in ALLOWED:
                yield f"Format {field.format!r} not allowed, must be one of {ALLOWED!r}"
