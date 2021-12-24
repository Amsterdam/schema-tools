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

Specific semantic validations are created by subclassing :class:`Validator`
and overriding the :meth:`Validator.validate` method.
Simply by means of subclassing,
the new validator is automatically registered.
Registered validator classes will all be run
when :meth:`Validator.run_all()` is invoked.

.. note::

   For the registration to work
   all :class:`Validator` subclasses need to be parsed by the Python interpreter.
   This can be achieved by importing the module they reside in.

Example:
    The following will run all registered validators
    on a dataset ``dataset``.
    Any validation errors are printed to ``stdout``::

        dataset = _get_dataset_schema(meta_schema_url, schema_location)
        validator = Validator(dataset=dataset)
        for error in validator.run_all():
            print(error)

"""
from __future__ import annotations

import operator
import re
from dataclasses import dataclass
from functools import partial
from typing import Callable, ClassVar, Iterator, List, Optional, Set, Type, cast, final

from schematools import MAX_TABLE_NAME_LENGTH
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


class Validator:
    """Base class for validators.

    Not only is this a base class for validators,
    it is also used for running all registered validators.
    See Also: :meth:`run_all`

    Registration is a side-effect of overriding this base class.

    """

    _registry: ClassVar[List[Type[Validator]]] = []
    dataset: DatasetSchema

    @classmethod
    def __init_subclass__(cls: Type[Validator]) -> None:
        """Register sub classes."""
        super().__init_subclass__()
        cls._registry.append(cls)

    def __init__(self, dataset: DatasetSchema) -> None:
        """Initialize the validator with a dataset.

        Args:
            dataset: The dataset to run the validations on.
        """
        self.dataset = dataset

    def validate(self) -> Iterator[ValidationError]:
        """Run validation."""
        raise NotImplementedError(
            f"{self.__class__.__name__}.{self.validate.__name__} should be overridden in "
            f"subclasses and called from there."
        )

    @final
    def run_all(self) -> Iterator[ValidationError]:
        r"""Run all registered validators.

        Yields:
            :class:`ValidationError`\s if any.

        """  # noqa: W605
        for validator_cls in self._registry:
            validator_inst = validator_cls(dataset=self.dataset)
            yield from validator_inst.validate()


class CamelCaseValidator(Validator):
    """Checks that conversion to snake case and back leaves field identifiers unchanged."""

    def validate(self) -> Iterator[ValidationError]:
        """Run validation."""
        for table in self.dataset.tables:
            for field in table.fields:
                error = _validate_camelcase(field.id)
                if error is not None:
                    yield error


def _validate_camelcase(ident: str) -> Optional[ValidationError]:
    if ident == "":
        return ValidationError("CamelCaseValidator", "empty identifier not allowed")
    camel = toCamelCase(to_snake_case(ident))
    if camel == ident:
        return None
    msg = f"{ident} does not survive conversion to snake case and back; suggestion: {camel}"
    return ValidationError("CamelCaseValidator", msg)


class PsqlIdentifierLengthValidator(Validator):
    """Validate inferred PostgreSQL table names for not exceeding max length.

    PostgreSQL has a maximum length for identifiers such as table names.
    We infer table names from dataset and table ids in the schemas.
    Those inferred table names should not exceed the max identifier length
    supported by PostgreSQL.
    """

    def validate(self) -> Iterator[ValidationError]:  # noqa: D102
        for table in self.dataset.get_tables(include_nested=True, include_through=True):
            db_name = table.db_name(
                with_dataset_prefix=True, with_version=True, check_assert=False
            )
            # print(f"{db_name:>{MAX_TABLE_NAME_LENGTH}}")
            if (length := len(db_name)) > MAX_TABLE_NAME_LENGTH:
                excess = length - MAX_TABLE_NAME_LENGTH
                yield ValidationError(
                    self.__class__.__name__,
                    f"Inferred PostgreSQL table name '{db_name}' is '{excess}' characters "
                    f"too long. Maximum table name length is '{MAX_TABLE_NAME_LENGTH}'. Define "
                    f"a `shortname`!",
                )


class IdentPropRefsValidator(Validator):
    """Validate that the identifier property refers to actual fields on the table definitions."""

    def validate(self) -> Iterator[ValidationError]:  # noqa: D102
        @dataclass
        class DerivedField:
            original: str
            derived: str

        for table in self.dataset.get_tables(include_nested=True):
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
                    fields, have = (
                        ("fields", "have") if len(missing_fields) > 1 else ("field", "has")
                    )
                    yield ValidationError(
                        self.__class__.__name__,
                        f"Property 'identifier' on table '{table.id}' refers to {fields} "
                        f"'{', '.join(missing_fields)}' that {have} not been defined on the "
                        "table.",
                    )


class ActiveVersionsValidator(Validator):
    """Validate activeVersions and table identifiers in referenced tables."""

    def validate(self) -> Iterator[ValidationError]:  # noqa: D102
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
        for tv in self.dataset["tables"]:
            assert isinstance(  # noqa: S101
                tv, TableVersions
            ), 'Someone messed with the internal representation of DatasetSchema["tables"].'
            for version, table in tv.active.items():
                assert isinstance(table, dict)  # noqa: S101
                if tv.id != table["id"]:
                    yield ValidationError(
                        self.__class__.__name__,
                        f"Table {tv.id!r} with version number '{version}' does not match with "
                        f"id {table['id']!r} of the referenced table.",
                    )
                version_in_table = SemVer(table["version"])
                if version != version_in_table:
                    yield ValidationError(
                        self.__class__.__name__,
                        f"Version number '{version}' in activeVersions for table {table['id']!r} "
                        f"does not match with version number '{version_in_table}' of the "
                        "referenced table.",
                    )
