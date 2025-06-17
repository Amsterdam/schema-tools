"""Python types for the Amsterdam Schema JSON file contents."""

from __future__ import annotations

import copy
import dataclasses
import json
import logging
import re
import sys
import typing
from collections import UserDict
from collections.abc import Callable, Iterator
from enum import Enum
from functools import cached_property, total_ordering
from re import Pattern
from typing import (
    Any,
    ClassVar,
    NamedTuple,
    NoReturn,
    TypeVar,
    Union,
    cast,
)

from schematools import MAX_TABLE_NAME_LENGTH
from schematools._utils import cached_method
from schematools.exceptions import (
    DatasetFieldNotFound,
    DatasetTableNotFound,
    DatasetVersionNotFound,
    LoaderNotFound,
    ParserError,
    SchemaObjectNotFound,
    ScopeNotFound,
)
from schematools.naming import to_snake_case, toCamelCase

if typing.TYPE_CHECKING:
    from schematools.loaders import CachedSchemaLoader

ST = TypeVar("ST", bound="SchemaType")
DTS = TypeVar("DTS", bound="DatasetTableSchema")
Json = Union[str, int, float, bool, None, dict[str, Any], list[Any]]
Ref = str

_PUBLIC_SCOPE = "OPENBAAR"

logger = logging.getLogger(__name__)

IS_DEBUGGER = (
    "pytest" in sys.modules
    or (sys.stdin and sys.stdin.isatty())
    or (hasattr(sys, "gettrace") and sys.gettrace() is not None)
)


class SemVer(str):
    """Semantic version numbers.

    Semantic version numbers take the form X.Y.Z
    where X, Y, and Z are non-negative integers,
    and MUST NOT contain leading zeroes.
    X is the major version,
    Y is the minor version,
    and Z is the patch version.
    Each element MUST increase numerically.
    For instance: 1.9.0 -> 1.10.0 -> 1.11.0.

    See also: https://semver.org/ (where the above "definition" was taken from)

    This class allows semantic version numbers to be prefixed with "v".
    Eg "v1.11.0".
    However,
    their canonical form,
    as outputted by the :meth:`__str__` and :meth:`__repr__` methods,
    will not include that prefix.

    In addition,
    the minor and patch version can be left unspecified.
    :class:`SemVer` will assume them to be equal to 0 in that case.

    This class was specifically made a subclass of :class:`str`
    to allow for seamless JSON serialization.
    """

    PAT: ClassVar[Pattern[str]] = re.compile(
        r"""
        ^v?                     # Optionally start with a 'v' (for version)
        (?P<major>\d+)          # A major version number is compulsory
        (?:\.                   # Optionally followed by a '.'
            (?P<minor>\d+)      # ... and a minor version number
            (?:\.               # Optionally followed by a '.'
                (?P<patch>\d+)  # ... and a patch version number
            )?
        )?$                     # And nothing else
        """,
        re.VERBOSE,
    )
    major: int
    minor: int
    patch: int

    def __init__(self, version: str) -> None:
        """Create a SemVer using a str that could be interpreted as an semantic version number.

        Examples:
            >>> SemVer("1.0.0")
            SemVer("1.0.0")

            >>> SemVer("v54")
            SemVer("54.0.0")

            >>> SemVer("v3.9.0")
            SemVer("3.9.0")

        Args:
            version: A semantic version number, optionally prefixed with a "v".

        Raises:
              ValueError if the string supplied is not a semantic version number.
        """
        if m := SemVer.PAT.match(version):
            self.major = int(m.group("major"))
            if minor_match := m.group("minor"):
                self.minor = int(minor_match)
                if patch_match := m.group("patch"):
                    self.patch = int(patch_match)
                else:
                    self.patch = 0
            else:
                self.minor = self.patch = 0
        else:
            raise ValueError(f"Argument '{version}' is not a semantic version number.")

    # IMPORTANT: All the comparisons operators have been overridden as we explicitly don't want
    # to fall back to the `str` versions. After all, we want the comparisons to only take the
    # numerical `major`, `minor` and `patch` versions into account and not the `str`
    # representation that might happen to include the prefix or the `str` representation of
    # numerical values ("10" < "2"). This is also the reason we can't rely on `@total_ordering`

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented

        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented

        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemVer):
            return False

        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __str__(self) -> str:
        """Return string representation of semantic version without a "v" prefix."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        """Return Python parseable representation of a SemVer instance."""
        return f'SemVer("{self!s}")'

    def __hash__(self) -> int:
        return hash(str(self))

    @property
    def signif(self) -> str:
        """Return stringified significant part of SemVer.

        Significant being the version numbers *without* the patch level.
        Stringified as in both significant numbers as a string separated by an underscore.

        Examples:
            >>> SemVer("v3.9.0").signif
            "3_9"
        """
        return f"{self.major}_{self.minor}"

    @property
    def vmajor(self) -> str:
        """Return stringified major part of SemVer.

        Examples:
            >>> SemVer("v3.9.0").vmajor
            "v3"
        """
        return f"v{self.major}"

    @property
    def is_production_version(self):
        return self.major >= 1


class JsonDict(UserDict):
    def json(self) -> str:
        return json.dumps(self.data)

    def json_data(self) -> Json:
        return json.loads(self.json())

    def __deepcopy__(self, memo):
        # This took a massive performance hit, as DRF was copying all attached objects.
        raise NotImplementedError(
            "Deepcopy of schema objects is not supported due to performance issues."
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __init__(self, *args, **kwargs):
        self.__initialized = False
        super().__init__(*args, **kwargs)
        self.__initialized = True

    def __setitem__(self, key, value):
        """Check for changes to the dictionary data. Note this is also called on __init__()."""
        if self.__initialized:
            logger.info("patching '%s' on %r id %d", key, self, id(self))
            property_name = to_snake_case(key)  # filterAuth / filter_auth
            if (
                property_name in self.__dict__
                and (prop := getattr(self.__class__, property_name, None)) is not None
                and isinstance(prop, cached_property)
            ):
                # Clear the @cached_property cache value
                del self.__dict__[property_name]

        super().__setitem__(key, value)

    if IS_DEBUGGER:

        def __missing__(self, key: str) -> NoReturn:
            # This method would also be called by self.get("auth") for example,
            # which makes such fail cases a bit slower than they have to be.
            raise KeyError(
                f"No field named '{key}' exists in {self.__class__.__name__},"
                f" available are: {', '.join(self.keys())}"
            )


class SchemaType(JsonDict):
    """Base class for top-level schema objects (dataset, table, profile, publisher, scope).

    Each object should have an "id" and "type" property.
    """

    def __hash__(self) -> int:
        return id(self)  # allow usage in lru_cache()

    @property
    def id(self) -> str:  # noqa: A003
        return cast(str, self["id"])

    @property
    def db_name(self) -> str:
        """The object name in a database-compatible format."""
        return to_snake_case(self.id)

    @cached_property
    def python_name(self) -> str:
        """The 'id', but snake-cased like a python variable.
        Some object types (e.g. dataset and table) may override this in classname notation.
        """
        return to_snake_case(self.id)

    @property
    def type(self) -> str:  # noqa: A003
        return cast(str, self["type"])

    @classmethod
    def from_dict(cls: type[ST], obj: Json) -> ST:
        return cls(copy.deepcopy(obj))


class DatasetSchema(SchemaType):
    """The schema of a dataset.

    This is a collection of JSON Schema's within a single file.
    """

    class Status(Enum):
        """The allowed status values according to the Amsterdam schema spec."""

        beschikbaar = "beschikbaar"
        niet_beschikbaar = "niet_beschikbaar"

    def __init__(
        self,
        data: dict,
        view_sql: str | None = None,
        loader: CachedSchemaLoader | None = None,
    ) -> None:
        """When initializing a datasets, a cache of related datasets
        can be added (at classlevel). Thus, we are able to get (temporal) info
        about the related datasets.

        Args:
            data: The JSON data from the file.
            view_sql: The SQL to create the view for this dataset.
            loader: The shared collection that the dataset should become part of.
                                This is used to resolve relations between different datasets.
        """
        if data.get("type") != "dataset" and not isinstance(data.get("versions"), dict):
            raise ValueError("Invalid Amsterdam Dataset schema file")

        self.view_sql = view_sql if view_sql is not None else None

        super().__init__(data)

        self._loader = loader
        if loader is not None:
            loader.add_dataset(self)  # done early for self-references

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self['id']}>"

    @property
    def loader(self) -> CachedSchemaLoader:
        if self._loader is None:
            raise LoaderNotFound(
                f"{self!r} has no loader defined, can't resolve requested object."
            )
        return self._loader

    @classmethod
    def from_dict(
        cls, obj: dict[str, Any], loader: CachedSchemaLoader | None = None
    ) -> DatasetSchema:
        """Parses given dict and validates the given schema."""
        return cls(obj, loader=loader)

    def json(
        self,
        inline_tables: bool = False,
        inline_publishers: bool = False,
        inline_scopes: bool = False,
    ) -> str:
        """Overwritten JSON logic that allows inlining of $refs"""
        if not inline_tables and not inline_publishers and not inline_scopes:
            return json.dumps(self.data)

        data = self.data.copy()

        # As a temporary fix add the status property of the default version, this can be
        # removed once the DSO-API doesn't rely on the status of a DatasetSchema, but on
        # the status of a DatasetVersionSchema.
        data["status"] = self.status.value

        if inline_tables:
            # Inline the tables in each version.
            for vmajor, version in self.versions.items():
                tables = version.get_tables()
                data["versions"][vmajor]["tables"] = [
                    t.json_data(inline_scopes=inline_scopes) for t in tables
                ]
        if inline_publishers and self.publisher is not None:
            data["publisher"] = self.publisher.json_data()
        if inline_scopes:
            data["auth"] = [s.json_data() if isinstance(s, Scope) else s for s in self.scopes]
        return json.dumps(data)

    def json_data(
        self,
        inline_tables: bool = False,
        inline_publishers: bool = False,
        inline_scopes: bool = False,
    ) -> Json:
        """Overwritten logic that inlines tables"""
        return json.loads(
            self.json(
                inline_tables=inline_tables,
                inline_publishers=inline_publishers,
                inline_scopes=inline_scopes,
            )
        )

    def get_view_sql(self) -> str:
        """Return the SQL for the view of the given table."""
        if self.view_sql is None:
            return None
        return self.view_sql

    def get_view_user(self) -> str:
        """Return the user that should be used to create the view for the given view"""
        return f"write_{self.db_name}"

    @cached_property
    def python_name(self) -> str:
        """The 'id', but camel cased like a class name."""
        return toCamelCase(self.id, first_upper=True)

    @property
    def title(self) -> str | None:
        """Title of the dataset (if set)"""
        return self.get("title")

    @property
    def description(self) -> str | None:
        """Description of the dataset (if set)."""
        return self.get("description")

    @property
    def license(self) -> str | None:
        """The license of the table as stated in the schema."""
        return self.get("license")

    @property
    def default_version(self) -> str:
        """Default version for this schema."""
        return self.get("defaultVersion", "v1")

    def _resolve_scope(self, auth):
        """Auth may contain a reference to a scope, or a list of such references.

        It may also be a string or list of strings.
        """
        if isinstance(auth, dict):
            if "$ref" in auth:
                return self.loader.get_scope(auth["$ref"])
            return Scope(auth)
        elif isinstance(auth, list):
            return [self._resolve_scope(a) for a in auth]
        else:
            return auth

    def _find_scope_by_id(self, id):
        all_scopes = self.loader.get_all_scopes()
        name = id.replace("/", "_").lower()
        scope = all_scopes.get(name)
        if not scope:
            raise ScopeNotFound(f"Scope {id} doesn't exist")
        return scope

    @cached_property
    def scopes(self) -> frozenset[Scope]:
        scopes = self._resolve_scope(self.get("auth"))
        if isinstance(scopes, Scope):
            return frozenset({scopes})
        elif isinstance(scopes, str):
            return frozenset({self._find_scope_by_id(scopes)})
        elif isinstance(scopes, list):
            return frozenset(
                [s if isinstance(s, Scope) else self._find_scope_by_id(s) for s in scopes]
            )
        return frozenset({self._find_scope_by_id(_PUBLIC_SCOPE)})

    @cached_property
    def auth(self) -> frozenset[str]:
        """Auth of the dataset, or OPENBAAR."""
        auth = self._resolve_scope(self.get("auth"))

        return _normalize_auth(auth)

    @property
    def status(self) -> DatasetSchema.Status:
        if self.get("versions"):
            return self.get_version(self.default_version).status
        else:
            value = self.get("status")
        try:
            return DatasetSchema.Status[value]
        except KeyError:
            raise ParserError(f"Status field contains an unknown value: {value}") from None

    def _get_dataset_schema(self, dataset_id: str) -> DatasetSchema:
        """Internal function to retrieve a (related) dataset from the shared cache."""
        if dataset_id == self.id:
            return self  # shortcut to avoid unneeded lookups

        # It's assumed here that the loader is a CachedSchemaLoader,
        # so data can be fetched multiple times.
        return self.loader.get_dataset(dataset_id)

    @cached_property
    def versions(self) -> dict[str, DatasetVersionSchema]:
        """Access the versions within the file."""
        return {
            vmajor: DatasetVersionSchema(version, vmajor, parent_schema=self)
            for vmajor, version in self.get("versions", {}).items()
        }

    def get_version(self, version) -> DatasetVersionSchema:
        """Get a specific version of the dataset."""
        try:
            return self.versions[version]
        except KeyError:
            raise DatasetVersionNotFound(
                f"Version {version} not found in dataset {self.id}. "
                f"Available versions are {list(self.versions)}"
            ) from None

    @property
    def has_an_available_version(self) -> bool:
        """Whether the Dataset has an available version which should be exposed on the API."""
        # TODO: use the lifecycleStatus once it has an `unavailable` status.
        for _, version in self.versions.items():
            if version.status == DatasetSchema.Status.beschikbaar:
                return True
        return False

    @property
    def tables(self) -> list[DatasetTableSchema]:
        """Access the tables within the file."""
        version = self.get_version(self.default_version)
        return version.get_tables()

    @cached_property
    def table_ids(self) -> list[str]:
        """Access different versions of the table, as mentioned in the dataset file."""
        return list(
            {table["id"] for version in self.get("versions") for table in version["tables"]}
        )

    @cached_property
    def publisher(self) -> Publisher | None:
        """Access the publisher within the file."""
        raw_publisher = self["publisher"]
        if isinstance(raw_publisher, str):
            # Compatibility with meta-schemas prior to 2.0
            try:
                publishers = self.loader.get_all_publishers()
                return [
                    publisher
                    for publisher in publishers.values()
                    if publisher["name"] == raw_publisher
                ][0]
            except (IndexError, LoaderNotFound):
                return None

        # From metaschema 2.0 it is an object { "$ref": "/publishers/ID" }.
        # First check if we already replaced the publisher in the schema.
        if "$ref" not in raw_publisher:
            return Publisher.from_dict(raw_publisher)

        publisher_id = raw_publisher["$ref"].split("/")[-1]
        return self.loader.get_publisher(publisher_id)

    def get_all_tables(
        self, include_nested: bool = False, include_through: bool = False
    ) -> list[DatasetTableSchema]:
        tables = {}
        for _, version in self.versions.items():
            version_tables = version.get_tables(
                include_nested=include_nested, include_through=include_through
            )
            for table in version_tables:
                if table.db_name not in tables:
                    tables[table.db_name] = table
        return tables.values()

    def get_tables(
        self,
        version: str | None = None,
        include_nested: bool = False,
        include_through: bool = False,
    ) -> list[DatasetTableSchema]:
        """List tables, including nested. Kept in place to provide backwards compatibility,
        but uses the specified (or default) version."""
        version = version if version else self.default_version
        version_schema = self.get_version(version)
        return version_schema.get_tables(
            include_nested=include_nested, include_through=include_through
        )

    @cached_method()  # type: ignore[misc]
    def get_table_by_id(
        self,
        table_id: str,
        version: str | None = None,
        include_nested: bool = True,
        include_through: bool = True,
    ) -> DatasetTableSchema:
        """Get table by id. Kept in place for backwards compatibility, but uses the
        specified (or default) version."""
        version = version if version else self.default_version
        version_schema = self.get_version(version)
        return version_schema.get_table_by_id(
            table_id, include_nested=include_nested, include_through=include_through
        )

    @property
    def nested_tables(self) -> list[DatasetTableSchema]:
        """Access list of nested tables."""
        return [f.nested_table for t in self.tables for f in t.fields if f.is_nested_table]

    @property
    def through_tables(self) -> list[DatasetTableSchema]:
        """Access list of through_tables, for n-m relations."""
        return [
            f.through_table
            for t in self.tables
            for f in t.fields
            if f.is_through_table and not (f.is_loose_relation and f.nm_relation is None)
        ]

    def build_nested_table(self, field: DatasetFieldSchema) -> DatasetTableSchema:
        """Construct an in-line table object for a nested field."""
        # Map Arrays into tables.
        table = field.table

        if "properties" not in field["items"]:
            raise KeyError(f"Key 'properties' not defined in '{table.id}.{field.id}'")

        # composite keys are concatened to one id an thus always strings
        parent_fk_type = "string" if len(table.identifier) > 1 else table.identifier_fields[0].type

        sub_table_schema = {
            "id": f"{table.id}_{field.id}",
            "originalID": field.id,
            "type": "table",
            "version": str(table.version),
            "auth": list((field.auth - {_PUBLIC_SCOPE}) or (table.auth - {_PUBLIC_SCOPE})) or None,
            "description": f"Auto-generated table for nested field: {table.id}.{field.id}",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "schema"],
                "properties": {
                    "id": {"type": "integer/autoincrement", "description": ""},
                    "schema": {"$ref": "#/definitions/schema"},
                    "parent": {
                        "type": parent_fk_type,
                        "relation": f"{self.id}:{table.id}",
                    },
                    **field["items"]["properties"],
                },
            },
        }

        # When shortnames are in use for table or field
        # we need to add a shortname to the dynamically generated
        # schema definition.
        if field.has_shortname or table.has_shortname:
            sub_table_schema["shortname"] = f"{table.shortname}_{field.shortname}"
        return DatasetTableSchema(
            sub_table_schema, parent_schema=self, _parent_table=table, nested_table=True
        )

    def build_through_table(self, field: DatasetFieldSchema) -> DatasetTableSchema:
        """Build the through table.

        The through tables are not defined separately in a schema.
        The fact that a M2M relation needs an extra table is an implementation aspect.
        However, the through (aka. junction) table schema is needed for the
        dynamic model generation and for data-importing.

        FK relations also have an additional through table, because the temporal information
        of the relation needs to be stored somewhere.

        For relations with an object-type definition of the relation, the
        fields for the source and target side of the relation are stored separately
        in the through table. E.g. for a M2M relation like this:

          "bestaatUitBuurten": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "identificatie": {
                  "type": "string"
                },
                "volgnummer": {
                  "type": "integer"
                }
              }
            },
            "relation": "gebieden:buurten",
            "description": "De buurten waaruit het object bestaat."
          }

        The through table has the following fields:
            - ggwgebieden_id
            - buurten_id
            - ggwgebieden_identificatie
            - ggwgebieden_volgnummer
            - bestaat_uit_buurten_identificatie
            - bestaat_uit_buurten_volgnummer
        """
        # Build the through_table for n-m relation
        # For relations, we have to use the real ids of the tables
        # and not the shortnames
        table = field.table
        left_dataset_id = self.id
        left_table_id = table.id

        # Both relation types can have a through table,
        # For FK relations, an extra through_table is created when
        # the table is temporal, to store the extra temporal information.
        relation = field.nm_relation
        if relation is None:
            relation = field.relation

        right_dataset_id, right_table_id = str(relation).split(":")[:2]

        target_field_id = field.id
        sub_table_schema: dict[str, Any] = {
            "id": f"{table.id}_{target_field_id}",
            "type": "table",
            "version": str(table.version),
            "auth": list((field.auth - {_PUBLIC_SCOPE}) or (field.table.auth - {_PUBLIC_SCOPE}))
            or None,
            "originalID": field.id,
            "throughFields": [left_table_id, target_field_id],
            "description": f"Auto-generated M2M table for {table.id}.{field.id}",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["schema", "id"],
                "properties": {
                    "schema": {"$ref": "#/definitions/schema"},
                    "id": {
                        "type": "integer/autoincrement",
                    },
                    left_table_id: {
                        "type": "string",
                        "relation": f"{left_dataset_id}:{left_table_id}",
                    },
                    target_field_id: {
                        "type": "string",
                        "relation": f"{right_dataset_id}:{right_table_id}",
                    },
                },
            },
        }

        # When shortnames are in use for table or field
        # we need to add a shortname to the dynamically generated
        # schema definition.
        if field.has_shortname or table.has_shortname:
            sub_table_schema["shortname"] = f"{table.shortname}_{field.shortname}"

        # We also need to add a shortname for the individual FK fields
        # pointing to left en right table in the M2M
        if field.has_shortname:
            sub_table_schema["schema"]["properties"][target_field_id][
                "shortname"
            ] = field.shortname
        if table.has_shortname:
            sub_table_schema["schema"]["properties"][left_table_id]["shortname"] = table.shortname

        # For both types of through tables (M2M and FK), we add extra fields
        # to the table (see docstring).
        if field.is_through_table:

            if field.is_object:
                properties = field.get("properties", {})
            elif field.is_array_of_objects:
                properties = field["items"].get("properties", {})
            else:
                properties = {}

            if table.has_composite_key and not field.is_loose_relation:
                # Change the spec of a relation inside a schema to an object.
                # Initially, the spec for the relation is based on a singular key.
                # When the relation has a composite key,
                # the spec needs to be expanded into an object.
                spec = sub_table_schema["schema"]["properties"][left_table_id]
                spec["type"] = "object"
                spec["properties"] = {
                    id_field.id: {"type": id_field.type} for id_field in table.identifier_fields
                }

            # We change the spec of the target field if the field is composite
            if not field.is_scalar and not field.is_loose_relation:
                spec = sub_table_schema["schema"]["properties"][target_field_id]
                spec["type"] = "object"
                spec["properties"] = properties

                # Any additional parts of the field (properties of the relation)
                # will be copied into the definition of the through table.
                sub_table_schema["schema"]["properties"] |= {
                    name: value
                    for name, value in properties.items()
                    if name not in field.related_table.identifier
                }

        return DatasetTableSchema(
            sub_table_schema, parent_schema=self, _parent_table=table, through_table=True
        )

    @property
    def related_dataset_schema_ids(self) -> set[str]:
        """Access the list or related schema ids.

        This property calculates the related data that are needed,
        so the users of this dataset can preload these datasets.
        This can also include the current dataset,
        for relations that point to other tables within the same dataset.
        """
        related_ids = set()
        # Nested tables are checked by walking over subfields.
        # Through tables are not checked, but don't have to as the "relation" is already checked.
        for table in self.tables:
            related_ids.update(table.related_dataset_ids)

        return related_ids


class DatasetVersionSchema(SchemaType):
    """
    Minimal implementation of DatasetVersionSchema to be able to work with
    Amsterdam Schema V3.
    """

    class LifecycleStatus(Enum):
        """The allowed lifecycle status values according to the Amsterdam schema spec."""

        stable = "stable"
        experimental = "experimental"

    def __init__(
        self,
        data: dict,
        vmajor: str,
        parent_schema: DatasetSchema,
    ):
        self.version = vmajor
        self._parent_schema = parent_schema
        super().__init__(data)

    @property
    def lifecycle_status(self) -> DatasetVersionSchema.LifecycleStatus | None:
        # Allow no value to provide backwards compatability
        value = self.data.get("lifecycleStatus")
        try:
            return DatasetVersionSchema.LifecycleStatus[value]
        except KeyError:
            raise ParserError(
                f"LifecycleStatus field contains an unknown value: {value}"
            ) from None

    @property
    def status(self) -> DatasetSchema.Status:
        value = self.data.get("status")
        try:
            return DatasetSchema.Status[value]
        except KeyError:
            raise ParserError(f"Status field contains an unknown value: {value}") from None

    @property
    def schema(self) -> DatasetSchema:
        if self._parent_schema is None:
            raise SchemaObjectNotFound(f"{self!r} doesn't have a parent schema defined.")
        return self._parent_schema

    @cached_property
    def tables(self) -> list[DatasetTableSchema]:
        """Access the tables within the file."""
        tables: list[DatasetTableSchema] = []
        for table_json in self.get("tables"):
            if "$ref" in table_json:
                # Dataset uses the new format to define table versions.
                # Load the default version
                table = self.schema.loader.get_table(self.schema, table_json["$ref"])
            else:
                # Old format, a single "dataset.json" with all tables embedded.
                # This format is also used when the dataset is serialized for database storage.
                table = DatasetTableSchema(table_json, parent_schema=self.schema)
            tables.append(table)

        return tables

    def get_tables(
        self,
        include_nested: bool = False,
        include_through: bool = False,
    ) -> list[DatasetTableSchema]:
        """List tables, including nested."""
        return list(
            self._get_tables(include_nested=include_nested, include_through=include_through)
        )

    @cached_method()  # type: ignore[misc]
    def get_table_by_id(
        self, table_id: str, include_nested: bool = True, include_through: bool = True
    ) -> DatasetTableSchema:
        snakecased_table_id = to_snake_case(table_id)
        tables = self.get_tables(include_nested=include_nested, include_through=include_through)
        for table in tables:
            if to_snake_case(table.id) == snakecased_table_id:
                return table

        available = "', '".join([table["id"] for table in tables])
        raise DatasetTableNotFound(
            f"Table '{table_id}' does not exist "
            f"in schema '{self._parent_schema.id}', available are: '{available}'"
        )

    def _get_tables(self, include_nested: bool = False, include_through: bool = False):
        # Using yield so nested/through tables aren't analyzed until they really have to.
        # This avoids unnecessary retrieval of related datasets/tables for get_table_by_id().
        yield from self.tables
        if include_nested:
            yield from self.nested_tables
        if include_through:
            yield from self.through_tables

    @property
    def nested_tables(self) -> list[DatasetTableSchema]:
        """Access list of nested tables."""
        return [f.nested_table for t in self.tables for f in t.fields if f.is_nested_table]

    @property
    def through_tables(self) -> list[DatasetTableSchema]:
        """Access list of through_tables, for n-m relations."""
        return [
            f.through_table
            for t in self.tables
            for f in t.fields
            if f.is_through_table and not (f.is_loose_relation and f.nm_relation is None)
        ]


class DatasetTableSchema(SchemaType):
    """The table within a dataset.
    This table definition follows the JSON Schema spec.

    This class has an `id` property (inherited from `SchemaType`) to uniquely
    address this dataset-table in the scope of the `DatasetSchema`.
    This `id` is used in lots of places in the dynamic model generation in Django.

    There is also a `db_name` method, that is used for the auto-generation
    of database table names. This also reads the `shortname`, to define
    a human-readable abbreviation that fits inside the maximum database table name length.
    """

    class LifecycleStatus(Enum):
        """The allowed lifecycle status values according to the Amsterdam schema spec."""

        stable = "stable"
        experimental = "experimental"

    def __init__(
        self,
        *args: Any,
        parent_schema: DatasetSchema,
        _parent_table: DatasetTableSchema | None = None,
        nested_table: bool = False,
        through_table: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._parent_schema = parent_schema
        self._parent_table = _parent_table
        self.nested_table = nested_table
        self.through_table = through_table

        if self.type != "table":
            raise ValueError(f"expected type = 'table', got {self.type!r}")

        if not self["schema"].get("$schema", "").startswith("http://json-schema.org/"):
            raise ValueError("Invalid JSON-schema contents of table")

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.qualified_id}>"

    def __eq__(self, other):
        return isinstance(other, DatasetTableSchema) and self.db_name == other.db_name

    def __hash__(self):
        return hash(self.db_name)

    def _resolve_scope(self, element):
        if "$ref" in element.get("auth", {}):
            element["auth"] = self.schema.loader.get_scope(element["auth"]["$ref"]).json_data()
        if isinstance(element.get("auth"), list):
            element["auth"] = [
                self.schema.loader.get_scope(a["$ref"]).json_data() if "$ref" in a else a
                for a in element["auth"]
            ]
        if element.get("type") == "object":
            # Nested fields may have their own auth.
            for sub_field in element.get("properties", {}).values():
                self._resolve_scope(sub_field)

    def json_data(self, inline_scopes: bool = False):
        data = super().json_data()

        if not inline_scopes:
            return data

        # Resolve auth on table level
        self._resolve_scope(data)

        # Resolve auths on the fields
        self._resolve_scope(data["schema"])

        return data

    @property
    def lifecycle_status(self) -> DatasetTableSchema.LifecycleStatus | None:
        # Allow no value to provide backwards compatability
        value = self.data.get("lifecycleStatus")
        if not value:
            return None
        try:
            return DatasetTableSchema.LifecycleStatus[value]
        except KeyError:
            raise ParserError(
                f"LifecycleStatus field contains an unknown value: {value}"
            ) from None

    @cached_property
    def qualified_id(self) -> str:
        """The fully qualified ID (for debugging)"""
        prefix = ""
        if self._parent_schema is not None:
            prefix = f"{self._parent_schema.id}."
        if self._parent_table is not None:
            prefix = f"{prefix}{self._parent_table.id}."
        return f"{prefix}{self['id']}.{self.version.vmajor}"

    @property
    def python_name(self) -> str:
        """The 'id', but camel cased like a class name."""
        return toCamelCase(self.id, first_upper=True)

    @property
    def is_view(self) -> bool:
        """Whether this table is a view."""
        return bool(self.get("derivedFrom", False))

    @property
    def derived_from(self) -> list:
        return self.get("derivedFrom", [])

    @property
    def shortname(self) -> str:
        """The shorter name if present, otherwise the ID.
        This is only used to generate human-readable database table names.
        """
        return self.get("shortname", self["id"])

    @property
    def title(self) -> str | None:
        """Title of the table."""
        return self.get("title")

    @property
    def has_shortname(self) -> bool:
        return self.get("shortname") is not None

    @property
    def crs(self) -> str:
        return self.get("crs") or self._parent_schema.get("crs")

    @property
    def dataset(self) -> DatasetSchema:
        """The dataset that this table is part of."""
        return self._parent_schema

    @property
    def parent_table(self) -> DatasetTableSchema | None:
        """The parent table of this table.

        For nested and through tables, the parent table is available.
        """
        return self._parent_table

    @property
    def parent_table_field(self) -> DatasetFieldSchema | None:
        """Provide the NM-relation that generated this through table."""
        if self.through_table or self.nested_table:
            return self._parent_table.get_field_by_id(self["originalID"])
        else:
            return None

    @cached_property
    def through_fields(self) -> tuple[DatasetFieldSchema, DatasetFieldSchema] | None:
        """Return the left and right side of an M2M through table.

        This only returns results when the table describes
        the intermediate table of an M2M relation (:attr:`is_through_table` is true).
        """
        field_ids = self.get("throughFields")
        if field_ids is None:
            return None

        return (
            self.get_field_by_id(field_ids[0]),
            self.get_field_by_id(field_ids[1]),
        )

    @cached_property
    def max_zoom(self) -> int:
        return self.get("zoom", {"max": 30})["max"]

    @cached_property
    def min_zoom(self) -> int:
        return self.get("zoom", {"min": 15})["min"]

    @property
    def description(self) -> str | None:
        """The description of the table as stated in the schema."""
        return self.get("description")

    @cached_property
    def fields(self) -> list[DatasetFieldSchema]:
        """All the fields of the table.

        This returns the direct fields that are part of the table.
        Fields that have "type=object" can define nested fields, which are not included here.
        These fields can either be read using ``field.subfields``, or be inlined
        using ``get_fields(include_subfields=True)``.
        """
        required = set(self["schema"]["required"])
        fields = [
            DatasetFieldSchema(
                _parent_table=self,
                _required=(id_ in required),
                **{**spec, "id": id_},
            )
            for id_, spec in self["schema"]["properties"].items()
        ]

        # If composite key, add PK field
        if self.has_composite_key and "id" not in self["schema"]["properties"]:
            # For temporal tables, we add an extra `faker` in the field definition
            # that knows how to concatenate the field of the composite key to generate an id
            kwargs = {"faker": "joiner"} if self.is_temporal else {}
            fields.insert(
                0,
                DatasetFieldSchema(
                    _parent_table=self, _required=True, type="string", id="id", **kwargs
                ),
            )

        return fields

    def get_fields(self, include_subfields: bool = False) -> Iterator[DatasetFieldSchema]:
        """Get the fields for this table.

        Args:
            include_subfields: This includes the subfields of ``object`` fields,
                               so those can be inlined in the main table. This is useful
                               for ORM and SQL databases, that can't support a nested structure.
        """
        if not include_subfields:
            # Shortcut, likely the code should just use table.fields.
            yield from self.fields
            return

        # Reuse the cache to avoid recreating the field objects.
        for field_schema in self.fields:
            if not field_schema.is_nested_object or field_schema.is_json_object:
                yield field_schema

            # When requested, expose the individual fields of a composite foreign keys.
            # These fields become part of the main table.
            if field_schema.is_composite_key or (
                field_schema.is_nested_object and not field_schema.is_json_object
            ):
                # Temporal date fields are excluded, they shouldn't be part into the main table.
                yield from (
                    subfield
                    for subfield in field_schema.subfields
                    if not subfield.is_temporal_range
                )

    def get_db_fields(self) -> Iterator[DatasetFieldSchema]:
        """Get the fields for this table that are included in the main table."""
        for field in self.get_fields(include_subfields=True):
            # Exclude nested and nm_relation fields,
            # and fields that are added only for temporality
            if (
                field.type.endswith("#/definitions/schema")
                or field.nm_relation
                or field.is_array_of_objects
                or field.is_nested_table
                or field.is_temporal_range
            ):
                continue
            yield field

    @cached_method()  # type: ignore[misc]
    def get_field_by_id(self, field_id: str) -> DatasetFieldSchema:
        """Get a fields based on the ids of the field."""
        for field_schema in self.fields:
            if field_schema.id == field_id:
                return field_schema

        raise DatasetFieldNotFound(f"Field '{field_id}' does not exist in table '{self.id}'.")

    @cached_method()  # type: ignore[misc]
    def get_additional_relation_by_id(self, relation_id: str) -> AdditionalRelationSchema:
        """Get the reverse relation based on the ids of the relation."""
        for additional_relation in self.additional_relations:
            if additional_relation.id == relation_id:
                return additional_relation

        raise DatasetFieldNotFound(
            f"Relation '{relation_id}' does not exist in table '{self.id}'."
        )

    @cached_property
    def display_field(self) -> DatasetFieldSchema | None:
        """Tell which fields can be used as display field."""
        display = self["schema"].get("display")
        return self.get_field_by_id(display) if display else None

    @cached_property
    def temporal(self) -> Temporal | None:
        """The temporal property of a Table.
        Describes validity of objects for tables where
        different versions of objects are valid over time.

        Temporal has an `identifier` property that refers to the attribute of objects in
        the table that uniquely identifies a specific version of an object
        from among other versions of the same object.

        Temporal also has a `dimensions` property, which gives the attributes of
        objects that determine for what (time)period an object is valid.
        """
        temporal_config = self.get("temporal")
        if temporal_config is None:
            return None

        identifier = temporal_config.get("identifier")
        dimensions = temporal_config.get("dimensions")
        if identifier is None or dimensions is None:
            raise ValueError("Invalid temporal data")

        try:
            return Temporal(
                identifier=identifier,
                identifier_field=self.get_field_by_id(identifier),
                dimensions={
                    key: TemporalDimensionFields(
                        self.get_field_by_id(start_field),
                        self.get_field_by_id(end_field),
                    )
                    for key, [start_field, end_field] in dimensions.items()
                },
            )
        except DatasetFieldNotFound as e:
            raise DatasetFieldNotFound(
                f"Error in '{self.id}' table; temporal identifier/range fields don't exist: {e}"
            ) from None

    @cached_property
    def is_temporal(self) -> bool:
        """Indicates if this is a table with temporal characteristics"""
        return "temporal" in self

    @cached_property
    def _temporal_range_field_ids(self) -> set[str]:
        """Provide all fields that are used for temporal ranges."""
        # Can't read self.temporal here, because that causes a loop
        # into get_field_by_id() / self.fields for a self-referencing table.
        # Reading raw dictionary data instead.
        try:
            dimensions = self["temporal"]["dimensions"]
        except KeyError:
            return set()

        date_fields = set()
        for _dimension, range_ids in dimensions.items():
            date_fields.update(range_ids)
        return date_fields

    @property
    def main_geometry(self) -> str:
        """The main geometry field, if there is a geometry field available.
        Default to "geometry" for existing schemas without a mainGeometry field.
        """
        return str(self["schema"].get("mainGeometry", "geometry"))

    @property
    def main_geometry_field(self) -> DatasetFieldSchema:
        """The main geometry as field object"""
        return self.get_field_by_id(self.main_geometry)

    @property
    def identifier(self) -> list[str]:
        """The main identifier field, if there is an identifier field available.
        Default to "id" for existing schemas without an identifier field.
        """
        identifier = self["schema"].get("identifier", ["id"])
        # Convert identifier to a list, to be backwards compatible with older schemas
        if not isinstance(identifier, list):
            identifier = [identifier]
        return identifier

    @cached_property
    def identifier_fields(self) -> list[DatasetFieldSchema]:
        """Return the field schema's for the identifier fields."""
        return [self.get_field_by_id(field_id) for field_id in self.identifier]

    @property
    def is_autoincrement(self) -> bool:
        """Return bool indicating autoincrement behaviour of the table identifier."""
        if self.has_composite_key:
            return False
        return self.get_field_by_id(self.identifier[0]).is_autoincrement

    @cached_property
    def has_composite_key(self) -> bool:
        """Tell whether the table uses multiple attributes together as it's identifier."""
        return len(self.identifier) > 1

    @property
    def has_parent_table(self) -> bool:
        """For nested or through tables, there is a parent table."""
        return self.nested_table or self.through_table

    @cached_property
    def additional_relations(self) -> list[AdditionalRelationSchema]:
        """Fetch list of additional (backwards or N-N) relations.

        This is a dictionary of names for existing forward relations
        in other tables with either the 'embedded' or 'summary'
        property
        """
        return [
            AdditionalRelationSchema(name, self, **relation)
            for name, relation in self["schema"].get("additionalRelations", {}).items()
        ]

    def get_reverse_relation(self, field: DatasetFieldSchema) -> AdditionalRelationSchema | None:
        """Find the description of a reverse relation for a field."""
        if not field.is_relation:
            raise ValueError("Field is not a relation")

        for relation in self.additional_relations:
            if relation.is_reverse_relation(field):
                return relation

        return None

    @property
    def schema(self) -> DatasetSchema:
        if self._parent_schema is None:
            raise SchemaObjectNotFound(f"{self!r} doesn't have a parent schema defined.")
        return self._parent_schema

    @cached_property
    def scopes(self) -> frozenset[Scope]:
        scopes = self.schema._resolve_scope(self.get("auth"))
        if isinstance(scopes, Scope):
            return frozenset({scopes})
        elif isinstance(scopes, str):
            return frozenset({self.schema._find_scope_by_id(scopes)})
        elif isinstance(scopes, list):
            return frozenset(
                [s if isinstance(s, Scope) else self.schema._find_scope_by_id(s) for s in scopes]
            )
        return frozenset({self.schema._find_scope_by_id(_PUBLIC_SCOPE)})

    @cached_property
    def auth(self) -> frozenset[str]:
        """Auth of the table, or OPENBAAR."""
        auth = self.get("auth")
        if (isinstance(auth, dict) and "$ref" in auth) or (
            isinstance(auth, list) and any("$ref" in a for a in auth)
        ):
            # Resolution of $ref is needed.
            resolved_auth = self.schema._resolve_scope(auth)
            return _normalize_auth(resolved_auth)

        return _normalize_auth(auth)

    @property
    def is_through_table(self) -> bool:
        """Indicate if table is an intersection table (n:m relation table) or base table."""
        return self.through_table

    @property
    def is_nested_table(self) -> bool:
        """Indicates if table is a nested table."""
        return self.nested_table

    @cached_property
    def has_geometry_fields(self) -> bool:
        return any(field.is_geo for field in self.fields)

    @cached_property
    def db_name(self) -> str:
        """Return the standard database name for the table.

        For some custom situations (e.g. importer, or handling table versions),
        use :meth:`db_name_variant`.
        """
        return self.db_name_variant()

    def db_name_variant(
        self,
        *,
        with_dataset_prefix: bool = True,
        with_version: bool = True,
        postfix: str = "",
        check_assert: bool = True,
    ) -> str:
        """Return derived table name for DB usage.

        Args:
            with_dataset_prefix: if True, include dataset ID as a prefix to the table name.
            with_version:  if True, include the major and minor version number in the table name.
            postfix: An optional postfix to append to the table name
            check_assert: Check max table length name. Can be turned of to have the check done
                by validation code (with much better error reporting.)

        Returns:
            A derived table name suitable for DB usage.

        """
        dataset_prefix = version_postfix = ""
        if with_version:
            version_postfix = f"{self.version.vmajor}"
        if with_dataset_prefix:
            dataset_prefix = to_snake_case(self.dataset.id)

        shortname = to_snake_case(self.shortname)
        if self.nested_table or self.through_table:
            # We don't automatically shorten user defined table names. Automatically generated
            # names, however, should be shortened as the user has no direct control over them.
            db_table_name = _name_join(dataset_prefix, shortname)
            additional_underscores = _name_join_count(dataset_prefix, shortname)
            max_length = (
                MAX_TABLE_NAME_LENGTH
                - len(version_postfix)
                - len(postfix)
                - additional_underscores
            )
            # Shortening should preserve both postfixes
            db_table_name = _name_join(db_table_name[:max_length], version_postfix) + postfix
        else:
            # User defined table name -> no shortening
            db_table_name = _name_join(dataset_prefix, shortname, version_postfix) + postfix

        # We are not shortening user defined table names automatically. Instead we rely on
        # validation code to prevent table ids in Amsterdam Schema's that result in DB table
        # names that are too long. Why? Haphazardly shortening names results in table names that
        # differ from what the user has specified in the corresponding Amsterdam Schema
        # potentially leading to confusion. It might also result in name clashes by turning a
        # previously unique name into a non-unique name by accidentally chopping off what made
        # the name unique. So we will have to do with an `assert` here, and fix Amsterdam
        # Schema's by specifying `shortname`s for whatever breaks.
        logger.debug(
            "Derived table name is '%s', its length: %d, max allowed length: %d",
            db_table_name,
            len(db_table_name),
            MAX_TABLE_NAME_LENGTH,
        )
        if check_assert and len(db_table_name) > MAX_TABLE_NAME_LENGTH:
            raise ValueError(
                f"table name {db_table_name!r} is too long, having {len(db_table_name)} chars. "
                f"Max allowed length is {MAX_TABLE_NAME_LENGTH} chars."
            )
        return db_table_name

    @property
    def version(self) -> SemVer:
        """Get table version."""
        # It's a required attribute, hence should be present.
        return SemVer(self["version"])

    @property
    def related_dataset_ids(self) -> set[str]:
        """Tell which dataset ID's relations point to.

        This can also include the current dataset,
        for relations that point to other tables within the same dataset.
        """
        related_ids = set()
        for field in self.fields:
            if relation := field.get("relation"):
                related_ids.add(relation.split(":", 1)[0])

            # Also inspect the subfields.
            # This does not use .get_fields(include_subfields=True) at the moment,
            # because that needs to inspect is_temporal_range, which then retrieves the
            # related table in return. That removes any need for loading other datasets:
            for subfield in field.subfields:
                if relation := subfield.get("relation"):
                    related_ids.add(relation.split(":", 1)[0])

        return related_ids

    @classmethod
    def from_dict(cls: type[DTS], obj: Json) -> DTS:  # noqa: D102
        raise Exception(
            f"A dict is not sufficient anymore to instantiate a {cls.__name__!r}. "
            "Use regular class instantiation instead and supply all required parameters!"
        )


def _name_join(*parts):
    """Combine items with underscores, and skip empty values."""
    return "_".join(filter(None, parts))


def _name_join_count(*parts):
    """Counts the number of underscores that _name_join inserts."""
    return len(list(filter(None, parts)))


class DatasetFieldSchema(JsonDict):
    """A single field (column) in a table."""

    def __init__(
        self,
        *args: Any,
        _parent_table: DatasetTableSchema | None,
        _parent_field: DatasetFieldSchema | None = None,
        _required: bool = False,
        _temporal_range: bool = False,
        **kwargs: Any,
    ) -> None:
        self._id: str = kwargs.pop("id")
        super().__init__(*args, **kwargs)
        self._parent_table = _parent_table
        self._parent_field = _parent_field
        self._required = _required
        self._temporal_range = _temporal_range

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.qualified_id}>"

    @property
    def table(self) -> DatasetTableSchema | None:
        """The table that this field is a part of"""
        return self._parent_table

    @property
    def parent_field(self) -> DatasetFieldSchema | None:
        """Provide access to the top-level field where it is a property for."""
        return self._parent_field

    @property
    def schema(self) -> DatasetSchema | None:
        if not self.table and self.table._parent_schema:
            raise SchemaObjectNotFound(f"{self!r} doesn't have a parent schema defined.")
        return self.table._parent_schema

    @cached_property
    def qualified_id(self) -> str:
        """The fully qualified ID (for debugging)"""
        prefix = ""
        if self._parent_table is not None:
            prefix = f"{self._parent_table.qualified_id}."
        if self._parent_field is not None:
            prefix = f"{prefix}{self._parent_field.id}."
        return f"{prefix}{self._id}"

    @property
    def id(self) -> str:
        """The id of a field uniquely identifies it among the fields of a table.

        Note that comparisons against id should be avoided when fields are
        retrieved using ``.get_fields(include_subfields=True)``. In such case,
        a subfield with a similar ID will match with the top-level field.
        """
        return self._id

    @property
    def is_autoincrement(self) -> bool:
        return "autoincrement" in self.type

    @cached_property
    def name(self) -> str:
        """The name as it is shown to the external world, camel-cased.
        In general, the "id" field is already camel-cased,
        but in case that didn't happen this property will fix that.
        For nested subfields (not a relation), the name needs to be prefixed
        for the external world.
        """
        name = self._id
        if self._parent_field is not None and self._parent_field.is_nested_object:
            name = f"{self._parent_field.name}_{name}"  # Note: recursive use of .name.
        return toCamelCase(name)

    @cached_property
    def python_name(self) -> str:
        """The name as its used internally in Python or an ORM, snake cased"""
        if self._parent_field is not None and self._parent_field.is_object:
            # Is a subfield, that's included in the same parent table.
            # (e.g. relationname_volgnummer)
            return to_snake_case(f"{self._parent_field.id}_{self._id}")
        else:
            return to_snake_case(self._id)

    @property
    def shortname(self) -> str:
        """The shorter name if present, otherwise the ID.
        Note this is only used to generate human-readable database table names.
        """
        return self.get("shortname", self._id)

    @cached_property
    def db_name(self) -> str:
        """Return the name that is being used in the database.
        This can be a different name then the internal name
        when the field is a relation, or has a short-name.
        """
        db_name = self.get("shortname", self._id)
        if self.relation is not None and not self.is_primary:
            # In schema foreign keys should be specified without _id,
            # but the db_column should be with _id
            # Regular foreign keys have a _id field for the database, just like Django ORM does.
            # ...but this is not done for primary keys that have a relationship constraint,
            # as those should read the original db column directly as their value.
            db_name += "_id"

        if self._parent_field is not None and self._parent_field.is_object:
            # Is a subfield, that's included in the same parent table.
            # (e.g. relationname_volgnummer)
            db_name = f"{self._parent_field.shortname}_{db_name}"

        return to_snake_case(db_name)

    @property
    def title(self) -> str | None:
        """Title of the field."""
        return self.get("title")

    @property
    def has_shortname(self) -> bool:
        """Reports whether this field has a shortname.

        You should never have to call this: name returns the shortname, if present.
        """
        return self.get("shortname") is not None

    @property
    def description(self) -> str | None:
        return self.get("description")

    @property
    def unit(self) -> str | None:
        """Unit of the field (if set)."""
        unit = self.get("unit")
        if isinstance(unit, dict):
            return unit.get("value", unit)
        return unit

    @property
    def description_with_unit(self) -> str | None:
        """Description of the field (if set), with unit (if set)."""
        if self.unit:
            return f"{self.description} [{self.unit}]"
        else:
            return self.description

    @property
    def faker(self) -> Faker | None:
        """Return faker name and properties used for mocking data."""
        faker = self.get("faker")
        if faker is None:
            return None
        elif isinstance(faker, str):
            return Faker(name=faker)
        else:
            name = faker.pop("name")
            return Faker(name=name, properties=faker)

    @property
    def required(self) -> bool:
        return self._required

    @cached_property
    def type(self) -> str:
        """Returns the type of this field.

        The type is one of the JSON Schema types "string", "integer", "number",
        "object", "array" or "boolean", or the URL of a schema defining a type
        (for geo types). "null" is never used by Amsterdam Schemas.

        Dates and URLs have type "string". Check the `format` to distinguish them
        from free-form text.

        See https://schemas.data.amsterdam.nl/docs/ams-schema-spec.html#data-types
        for details.
        """
        value = self.get("type")
        if not value:
            value = self.get("$ref")
            if not value:
                raise RuntimeError(f"No 'type' or '$ref' found in {self!r}")
        return str(value)

    @cached_property
    def is_primary(self) -> bool:
        """When name is 'id' the field should be the primary key
        For composite keys (table.identifier has > 1 item), an 'id'
        field is autogenerated.
        """
        if self.is_subfield:
            return False
        return self.id == "id" or [self.id] == self._parent_table.identifier

    @cached_property
    def is_identifier_part(self) -> bool:
        """Tell whether the field is part of the composite primary key"""
        # This logic currently won't apply to subfields,
        # otherwise a "relation.identifier" could match the "in" clause as well.
        return (
            self.id == "id" or self._id in self._parent_table.identifier
        ) and self._parent_field is None

    @cached_property
    def is_internal(self) -> bool:
        """Id fields for table with composite key is only for internal (Django) use."""
        return self.is_primary and self._parent_table.has_composite_key

    @cached_property
    def is_relation(self) -> bool:
        """Tell whether the field is a relation.
        This is a convenience for ``bool(field.relation or field.nm_relation)``.
        """
        return bool(self.get("relation"))

    @cached_property
    def relation(self) -> str | None:
        """Give the 1:N relation, if it exists."""
        if self.type == "array":
            return None
        return self.get("relation")

    @cached_property
    def nm_relation(self) -> str | None:
        """Give the N:M relation, if it exists (called M2M in Django)."""
        if self.type != "array":
            return None
        return self.get("relation")

    @cached_property
    def related_table(self) -> DatasetTableSchema | None:
        """If this field is a relation, return the table this relation references."""
        relation = self.get("relation")  # works for both 1:N and N:M relations
        if not relation:
            return None
        # Find the related field
        related_dataset_id, related_table_id = relation.split(":")
        dataset = self.table.dataset
        try:
            dataset = dataset._get_dataset_schema(related_dataset_id)
            return dataset.get_table_by_id(
                related_table_id, include_nested=False, include_through=False
            )
        except SchemaObjectNotFound as e:
            # Amend the error message for better debugging
            raise e.__class__(
                f"Unable to resolve relation '{relation}' for field '{self.qualified_id}': {e}"
            ) from e

    @property
    def related_field_ids(self) -> list[str] | None:
        """For a relation field, returns the identifiers of the referenced fields.

        The returned list contains only the fields, e.g., ["id", "volgnummer"].
        These are fields on the table `self.related_table`.

        For loose relations, it will only return
        the first field of the related table.

        If self is not a relation field, the return value is None.
        """
        if not self.get("relation"):
            return None
        elif self.is_object:
            # Relation where the fields are defined as sub-fields
            return [
                subfield_id
                for subfield_id, subfield in self["properties"].items()
                if subfield.get("format") not in ("date", "date-time")
            ]
        elif self.is_loose_relation:
            return self.related_table.identifier[:1]
        else:
            # References the primary key of the related table.
            return self.related_table.identifier

    @property
    def related_fields(self) -> list[DatasetFieldSchema] | None:
        """Convenience property that returns the related field schemas."""
        ids = self.related_field_ids
        if ids is None:
            return None
        else:
            return [self.related_table.get_field_by_id(id_name) for id_name in ids]

    @property
    def reverse_relation(self) -> AdditionalRelationSchema | None:
        """Find the opposite description of a relation.

        When there is a relation, this only returns a description
        when the linked table also describes the other end of relationship.
        """
        related_table = self.related_table
        if related_table is None:
            return None

        return related_table.get_reverse_relation(self)

    @property
    def format(self) -> str | None:
        return self.get("format")

    @property
    def multipleof(self) -> float | None:
        return self.get("multipleOf")

    @cached_property
    def is_object(self) -> bool:
        """Tell whether the field references an object.
        This might also be a relation, with a composite key.
        In both cases, the object subfields could be inlined in the main SQL table.
        See also: ``is_nested_object`` and ``is_composite_key``.
        """
        return self.get("type") == "object"

    @cached_property
    def is_scalar(self) -> bool:
        """Tell whether the field is a scalar."""
        return self.get("type") not in {"object", "array"}

    @cached_property
    def is_subfield(self) -> bool:
        """Tell whether this field is part of an embedded object (e.g. temporal relation)"""
        return self._parent_field is not None

    @property
    def is_temporal_range(self) -> bool:
        """Tell whether the field is used to store the range of a temporal dimension.
        (e.g. beginGeldigheid or eindGeldigheid).
        """
        # Checks whether this is a subfield, that is mentioned
        # by the related table as a temporal range field.
        return (
            self._parent_field is not None
            # The format check is a performance improvement,
            # to avoid unnecessary loading the related table.
            and self.get("format") in ("date", "date-time")
            and self._parent_field.get("relation")
            and self.id in self._parent_field.related_table._temporal_range_field_ids
        )

    @cached_property
    def is_geo(self) -> bool:
        """Tell whether the field references a geo object."""
        return "geojson.org" in self.get("$ref", "")

    @property
    def crs(self) -> str | None:
        """CRS for this field, or None if not a geo field."""
        if not self.is_geo:
            return None
        if self.table:
            return self.get("crs") or self.table.get("crs") or self.table.dataset.get("crs")
        return self.get("crs")

    @cached_property
    def srid(self) -> int | None:
        """The integer value for the spatial reference ID (for geometry fields)."""
        # Note that it still requires EPSG:### notation, and none of the other URN formats
        # that the gisserver CRS class parses (which uses GDAL / Django's SpatialReference for it)
        crs = self.crs
        return int(crs.split("EPSG:")[1]) if crs else None

    @cached_property
    def provenance(self) -> str | None:
        """Get the provenance info, if available, or None."""
        return self.get("provenance")

    @property
    def field_items(self) -> Json | None:
        """Return the item definition for an array type."""
        return self.get("items", {}) if self.is_array else None

    @cached_method()  # type: ignore[misc]
    def get_field_by_id(self, field_id: str) -> DatasetFieldSchema:
        """Finds and returns the subfield with the given id.

        DatasetFieldNotFound is raised when the field does not exist.
        """
        for field_schema in self.subfields:
            if field_schema.id == field_id:
                return field_schema

        name = self.table.id + "." + self.id
        raise DatasetFieldNotFound(f"Subfield {field_id!r} does not exist in field {name!r}.")

    @cached_property
    def subfields(self) -> list[DatasetFieldSchema]:
        """Return the subfields for a nested structure.

        For a nested object, fields are based on its properties,
        for an array of objects, fields are based on the properties
        of the "items" field.

        When subfields are added as part of an 1m-relation
        those subfields need to be prefixed with the name of the relation field.
        However, this is not the case for the so-called `dimension` fields
        of a temporal relation (e.g. `beginGeldigheid` and `eindGeldigheid`).

        If self is not an object or array, the return value is an empty iterator.
        """
        if self.is_object:
            # Field has direct subfields (type=object)
            required = set(self.get("required", []))
            properties = self.get("properties", {})
        elif self.is_array_of_objects and self.field_items is not None:
            # Field has an array of objects (type=array, items are objects)
            required = set(self.field_items.get("required") or ())
            properties = self.field_items["properties"]
        else:
            return []

        return [
            DatasetFieldSchema(
                _parent_table=self._parent_table,
                _parent_field=self,
                _required=(id_ in required),
                **{**spec, "id": id_},
            )
            for id_, spec in properties.items()
        ]

    @cached_property
    def is_array(self) -> bool:
        """Checks if field is an array field."""
        return self.get("type") == "array"

    @cached_property
    def is_array_of_objects(self) -> bool:
        """Checks if field is an array of objects."""
        return self.is_array and self.get("items", {}).get("type") == "object"

    @cached_property
    def is_array_of_scalars(self) -> bool:
        """Checks if field is an array of scalars."""
        return self.is_array and self.get("items", {}).get("type") != "object"

    @cached_property
    def is_nested_table(self) -> bool:
        """Checks if field is a possible nested table."""
        return self.is_array_of_objects and self.nm_relation is None

    @cached_property
    def is_nested_object(self) -> bool:
        """Checks if field is a possible nested object definition."""
        return self.is_object and not self.get("relation")

    @cached_property
    def is_json_object(self) -> bool:
        """Checks if field is a json object."""
        return self.format == "json"

    @cached_property
    def nested_table(self) -> DatasetTableSchema | None:
        """Access the nested table that this field needs to store its data."""
        if not self.is_nested_table:
            return None
        return self._parent_table.dataset.build_nested_table(field=self)

    @cached_property
    def is_through_table(self) -> bool:
        """Checks if field is a possible through table.

        NM tables always are through tables. For 1N tables, there is a through
        tables if the relations has additional attributes that are not part of the key,
        or if the relation is temporal.
        """
        return (
            self.nm_relation is not None
            or self.relation_attributes
            or (
                # Uses a temporal relationship:
                self.relation is not None
                and (
                    # The "is_composite_key" check is a performance win,
                    # as that avoids having to fetch the related table object.
                    self.is_composite_key
                    or self.related_table.is_temporal
                )
            )
        )

    @cached_property
    def relation_attributes(self) -> set[str]:
        """Check if the relation has additional attributes.

        Some relations have attributes that are not part of key,
        but are only attributed to the relation itself,
        e.g. `beginGeldigheid`.
        """
        if self.get("relation") is None:
            return set()
        return {sf.id for sf in self.subfields} - set(self.related_table.identifier)

    @cached_property
    def through_table(self) -> DatasetTableSchema | None:
        """Access the through table that this fields needs to store its data."""
        if not self.is_through_table:
            return None
        return self._parent_table.dataset.build_through_table(field=self)

    @cached_property
    def scopes(self) -> frozenset[Scope]:
        scopes = self.schema._resolve_scope(self.get("auth"))
        if isinstance(scopes, Scope):
            return frozenset({scopes})
        elif isinstance(scopes, str):
            return frozenset({self.schema._find_scope_by_id(scopes)})
        elif isinstance(scopes, list):
            return frozenset(
                [s if isinstance(s, Scope) else self.schema._find_scope_by_id(s) for s in scopes]
            )
        return frozenset({self.schema._find_scope_by_id(_PUBLIC_SCOPE)})

    @cached_property
    def auth(self) -> frozenset[str]:
        """Auth of the field, or OPENBAAR.

        When the field is a subfield, the auth has been defined on
        the parent field, so we need to return the auth of the parent field.
        """
        # As this call is made so many times, this is a cached_property
        # to avoid hitting __missing__() too many times.
        if self.is_subfield:
            return self.parent_field.auth
        auth = self.get("auth")
        if (isinstance(auth, dict) and "$ref" in auth) or (
            isinstance(auth, list) and any("$ref" in a for a in auth)
        ):
            # Resolution of $ref is needed.
            resolved_auth = self.schema._resolve_scope(auth)
            return _normalize_auth(resolved_auth)

        return _normalize_auth(auth)

    @cached_property
    def filter_auth(self) -> frozenset[str]:
        """Auth of the field, or OPENBAAR.
        This setting allows denying access to query/search on fields,
        e.g. to block searching all buildings from a certain owner.
        """
        if self.is_subfield:
            return self.parent_field.filter_auth
        return _normalize_auth(self.get("filterAuth"))

    @cached_property
    def is_composite_key(self):
        """Tell whether the relation uses a composite key"""
        return self.get("relation") and self.is_object and len(self.related_table.identifier) > 1

    @property
    def is_loose_relation(self):
        """Determine if relation is loose or not."""
        related_table = self.related_table

        # Short-circuit for non-temporal or on-the-fly (through or nested) schemas
        # NOTE: this logic also breaks testing for loose relations on through tables!
        if (
            related_table is None
            or not related_table.is_temporal
            or self._parent_table.is_through_table
            or self._parent_table.is_nested_table
        ):
            return False

        # If temporal, this implicates that the type is not a scalar
        # but needs to be more complex (object) or array_of_objects
        if self.type in ("string", "integer") or self.is_array_of_scalars:
            return True

        # So, target-side of relation is temporal
        # Determine fieldnames used for temporal
        # Table identifier is mandatory and always contains at least one field
        identifier_field = related_table.identifier_fields[0]
        sequence_field = related_table.temporal.identifier_field

        if self.is_array_of_objects:
            properties = self.field_items["properties"]
        elif self.is_object:
            properties = self["properties"]
        else:
            raise ValueError("Relations should have string/array/object type")

        source_type_set = {
            (prop_name, prop_val["type"])
            for prop_name, prop_val in properties.items()
            if prop_val.get("format") != "date-time"  # exclude beginGeldigheid/eindGeldigheid?
        }
        destination_type_set = {
            (identifier_field.name, identifier_field.type),
            (sequence_field.name, sequence_field.type),
        }

        # If all fields of source_type_set are also in destination_type_set
        # it is not a loose relation
        # truth table is:
        # destination_type_set  source_type_set  result (meaning: relation is loose)
        #  {1, 2}                   {1}          True
        #  {1, 2}                   {1, 2}       False
        #  {1, 2}                   {0}          True
        return not destination_type_set <= source_type_set


class AdditionalRelationSchema(JsonDict):
    """Data class describing the additional relation block."""

    def __init__(self, _id: str, _parent_table: DatasetTableSchema | None = None, **kwargs):
        super().__init__(**kwargs)
        self._id = _id
        self._parent_table = _parent_table

    def __repr__(self) -> str:
        prefix = ""
        if self._parent_table is not None:
            prefix = f"{self._parent_table.id}."
        return f"<{self.__class__.__name__}: {prefix}{self._id}>"

    @property
    def id(self):
        return self._id

    @property
    def name(self) -> str:
        return toCamelCase(self._id)

    @cached_property
    def python_name(self) -> str:
        return to_snake_case(self._id)

    @property
    def parent_table(self):
        return self._parent_table

    def is_reverse_relation(self, field: DatasetFieldSchema):
        """See whether this relation"""
        # TODO: should the "additionalRelations" use separate fields for table/field?
        # Everywhere else the relation is described using dataset:table:field.
        table = field.table
        return (
            table.dataset.id == self._parent_table.dataset.id
            and self["table"] == table.id
            and self["field"] == field.name
        )

    @property
    def relation(self) -> str:
        """Relation identifier."""
        # Currently generated, will change in schema later
        return f"{self._parent_table.dataset.id}:{self['table']}:{self['field']}"

    @cached_property
    def related_table(self) -> DatasetTableSchema:
        """Return the table this relation references."""
        # NOTE: currently doesn't cross datasets
        try:
            return self._parent_table.dataset.get_table_by_id(
                self["table"], include_nested=False, include_through=False
            )
        except DatasetTableNotFound as e:
            raise RuntimeError(f"Unable to resolve {self}.related_table: {e}") from e

    @cached_property
    def related_field(self) -> DatasetFieldSchema:
        """Return the field this reverse relation queries to find objects."""
        return self.related_table.get_field_by_id(self["field"])

    @property
    def format(self):
        """Format: "summary" or "embedded"."""
        return self.get("format", "summary")


@total_ordering
class PermissionLevel(Enum):
    """The various levels that can be provided on specific fields."""

    # Higher values give higher preference. The numbers are arbitrary and for internal usage
    # allowing to test test "read > encoded" for example.
    READ = 50
    ENCODED = 40
    RANDOM = 30
    LETTERS = 10
    SUBOBJECTS_ONLY = 1  # allows to open a table only to access sub-fields
    NONE = 0  # means no permission.

    highest = READ

    @classmethod
    def from_string(cls, value: str | None) -> PermissionLevel:
        """Cast the string value to a permission level object."""
        if value is None:
            return cls.NONE
        elif "_" in value:
            # Anything with an underscore is internal
            raise ValueError("Invalid permission")
        else:
            return cls[value.upper()]

    def __str__(self) -> str:
        # Using the name as official value.
        return self.name

    def __bool__(self) -> bool:
        """The 'none' level is recognized as "NO PERMISSION"."""
        # more direct then reading bool(self.value) as that goes through descriptors
        return self is not PermissionLevel.NONE

    def __lt__(self, other) -> bool:
        if not isinstance(other, PermissionLevel):
            return NotImplemented

        return self.value < other.value


@dataclasses.dataclass(order=True)
class Permission:
    """The result of an authorisation check.

    The extra fields in this dataclass are mainly provided for debugging purposes.
    The dataclass can also be ordered; they get sorted by access level.
    """

    #: The permission level given by the profile
    level: PermissionLevel

    #: The extra parameter for the level (e.g. "letters:3")
    sub_value: str | None = None

    #: Who authenticated this (added for easier debugging. typically tested against)
    source: str | None = dataclasses.field(default=None, compare=False)

    def __post_init__(self) -> None:
        if self.level is PermissionLevel.NONE:
            # since profiles only grant permission,
            # having no permission is always from the schema.
            self.source = "schema"

    @classmethod
    def from_string(cls, value: str | None, source: str | None = None) -> Permission:
        """Cast the string value to a permission level object."""
        if value is None:
            return cls(PermissionLevel.NONE, source=source)

        parts = value.split(":", 1)  # e.g. letters:3
        return cls(
            level=PermissionLevel.from_string(parts[0]),
            sub_value=(parts[1] if len(parts) > 1 else None),
            source=source,
        )

    def __bool__(self):
        return bool(self.level)

    def transform_function(self) -> Callable[[Json], Json] | None:
        """Adjust the value, when the permission level requires this.
        This is needed for "letters:3", and things like "encoded".
        """
        if self.level is PermissionLevel.READ:
            return None
        elif self.level is PermissionLevel.LETTERS:
            return lambda value: value[0 : int(self.sub_value)]
        else:
            raise NotImplementedError(f"Unsupported permission mode: {self.level}")


Permission.none = Permission(level=PermissionLevel.NONE)


class ProfileSchema(SchemaType):
    """The complete profile object.

    It contains the :attr:`scopes` that the user should match,
    and definitions for various :attr:`datasets`.
    """

    @classmethod
    def from_file(cls, filename: str) -> ProfileSchema:
        """Open an Amsterdam schema from a file."""
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Json) -> ProfileSchema:
        """Parses given dict and validates the given schema"""
        return cls(copy.deepcopy(obj))

    @property
    def name(self) -> str | None:
        """Name of Profile (if set)"""
        return self.get("name")

    @cached_property
    def scopes(self) -> frozenset[str]:
        """All these scopes should match in order to activate the profile."""
        return _normalize_auth(self.get("scopes"))

    @cached_property
    def datasets(self) -> dict[str, ProfileDatasetSchema]:
        """The datasets that this profile provides additional access rules for."""
        return {
            id: ProfileDatasetSchema(id, self, data)
            for id, data in self.get("datasets", {}).items()
        }


class ProfileDatasetSchema(JsonDict):
    """A schema inside the profile dataset.

    It grants :attr:`permissions` to a dataset on a global level,
    or more fine-grained permissions to specific :attr:`tables`.
    """

    def __init__(
        self,
        _id: str,
        _parent_schema: ProfileSchema,
        data: Json,
    ) -> None:
        super().__init__(data)
        self._id = _id
        self._parent_schema = _parent_schema

    @property
    def id(self) -> str:
        return self._id

    @property
    def profile(self) -> ProfileSchema | None:
        """The profile that this definition is part of."""
        return self._parent_schema

    @cached_property
    def permissions(self) -> Permission:
        """Global permissions that are granted to the dataset. e.g. "read"."""
        return Permission.from_string(
            self.get("permissions"), source=f"profiles[{self._id}].dataset"
        )

    @cached_property
    def tables(self) -> dict[str, ProfileTableSchema]:
        """The tables that this profile provides additional access rules for."""
        return {
            id: ProfileTableSchema(id, self, data) for id, data in self.get("tables", {}).items()
        }


class ProfileTableSchema(JsonDict):
    """A single table in the profile.

    This grants :attr:`permissions` to a specific table,
    or more fine-grained permissions to specific :attr:`fields`.
    When the :attr:`mandatory_filtersets` is defined, the table may only
    be queried when a specific search query parameters are issued.
    """

    def __init__(
        self,
        _id: str,
        _parent_schema: ProfileDatasetSchema,
        data: Json,
    ) -> None:
        super().__init__(data)
        self._id = _id
        self._parent_schema = _parent_schema

    @property
    def id(self) -> str:
        return self._id

    @property
    def dataset(self) -> ProfileDatasetSchema | None:
        """The profile that this definition is part of."""
        return self._parent_schema

    @cached_property
    def permissions(self) -> Permission:
        """Global permissions that are granted for the table, e.g. "read"."""
        permissions = self.get("permissions")
        source = (
            f"profiles[{self._parent_schema.profile.name}].datasets"
            f".{self._parent_schema.id}.tables.{self._id}"
        )

        if not permissions:
            if self.get("fields"):
                # There are no global permissions on the table, but some fields can be read.
                # Hence this gives indirect permission to access the table.
                # The return value expresses this, to avoid complex rules in the permission checks.
                return Permission(PermissionLevel.SUBOBJECTS_ONLY, source=f"{source}.fields.*")

            raise RuntimeError(
                f"Profile table {source} is invalid: "
                f"no permissions are given for the table or field."
            )
        else:
            return Permission.from_string(permissions, source=source)

    @cached_property
    def fields(self) -> dict[str, Permission]:
        """The fields with their granted permission level.

        This can be "read" or things like "letters:3".
        """
        source_table = (
            f"profiles[{self._parent_schema.profile.name}].datasets"
            f".{self._parent_schema.id}.tables.{self._id}"
        )
        return {
            name: Permission.from_string(value, source=f"{source_table}.fields.{name}")
            for name, value in self.get("fields", {}).items()
        }

    @property
    def mandatory_filtersets(self) -> list[list[str]]:
        """Tell whether the listing can only be requested with certain inputs.

        E.g., an API user may only list data when they supply the lastname + birthdate.

        Example value::

            [
              ["bsn", "lastname"],
              ["postcode", "regimes.aantal[gte]"]
            ]
        """
        return self.get("mandatoryFilterSets", [])


class TemporalDimensionFields(NamedTuple):
    """A tuple that describes the fields for start field and end field of a range.

    This could be something like ``("beginGeldigheid", "eindGeldigheid")``.
    """

    start: DatasetFieldSchema
    end: DatasetFieldSchema


@dataclasses.dataclass
class Temporal:
    """The temporal property of a Table.

    Describes validity of objects for tables where
    different versions of objects are valid over time.

    Attributes:
        identifier:
            The key to the property that uniquely identifies a specific
            version of an object from among other versions of the same object.

            This property combined with the fixed identifier forms a unique key for an object.

            These identifier properties are non-contiguous increasing integers.
            The latest version of an object will have the highest value for identifier.

        dimensions:
            Contains the attributes of objects that determine for what (time)period
            an object is valid.

            Dimensions is of type dict.
            A dimension is a tuple of the form "('valid_start', 'valid_end')",
            describing a closed set along the dimension for which an object is valid.

            Example:
                With dimensions = {"time":('valid_start', 'valid_end')}
                an_object will be valid on some_time if:
                an_object.valid_start <= some_time < an_object.valid_end
    """

    identifier: str
    identifier_field: DatasetFieldSchema
    dimensions: dict[str, TemporalDimensionFields] = dataclasses.field(default_factory=dict)

    @cached_property
    def temporal_fields(self) -> list[DatasetFieldSchema]:
        """Return the fields that are required for this temporal property."""
        return [field for fields in self.dimensions.values() for field in fields]


def _normalize_auth(auth: None | str | list | tuple | dict) -> frozenset[str]:
    """Make sure the auth field has a consistent type."""
    if not auth:
        # No auth implies OPENBAAR.
        return frozenset({_PUBLIC_SCOPE})
    elif isinstance(auth, (list, tuple, set)):
        # Multiple scopes act choices (OR match).
        auths = [a.id if isinstance(a, Scope) else a for a in auth]
        return frozenset(auths)
    elif isinstance(auth, Scope):
        # Auth can be a scope object, with an id, name, and owner.
        return frozenset({auth.id})
    else:
        # Normalize single scope to set return type too.
        return frozenset({auth})


@dataclasses.dataclass
class Faker:
    """Name and properties that can be used for mock data."""

    name: str
    properties: dict[str, Any] = dataclasses.field(default_factory=dict)


class Publisher(SchemaType):
    @classmethod
    def from_file(cls, filename: str) -> Publisher:
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Json) -> Publisher:
        return cls(copy.deepcopy(obj))


class Scope(SchemaType):
    """
    A Scope is a simple object that describes a scope in the Amsterdam Schema.

    Scopes determine the access level of a user to a specific dataset, table, or field.

    It has an ID, a name, and an owner (which refers to a Publisher).
    """

    def __str__(self) -> str:
        return self.id

    def __eq__(self, other):
        if not isinstance(other, Scope):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @property
    def db_name(self) -> str:
        """The object name in a database-compatible format."""
        return to_snake_case(self.id.lower())

    @cached_property
    def python_name(self) -> str:
        """The 'id', but snake-cased like a python variable.
        Some object types (e.g. dataset and table) may override this in classname notation.
        """
        return to_snake_case(self.id.lower())

    @property
    def name(self) -> str:
        return self.get("name", "")

    @property
    def owner(self) -> dict:
        return self.get("owner", {})

    @property
    def accessPackages(self) -> dict[str, str]:
        return self.get("accessPackages", {})

    @property
    def productionPackage(self) -> str:
        return self.accessPackages.get("production", "")

    @property
    def nonProductionPackage(self) -> str:
        return self.accessPackages.get("nonProduction", "")

    @classmethod
    def from_file(cls, filename: str) -> Scope:
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Json) -> Scope:
        return cls(copy.deepcopy(obj))

    @classmethod
    def from_string(cls, id: str) -> Scope:
        return cls({"id": id, "name": id, "owner": {}})
