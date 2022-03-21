"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import copy
import json
import logging
import re
import warnings
from collections import UserDict
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property, total_ordering
from json import JSONEncoder
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    NoReturn,
    Optional,
    Pattern,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

import jsonschema
from deprecated import deprecated
from jsonschema import draft7_format_checker
from methodtools import lru_cache
from more_itertools import first

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR
from schematools.datasetcollection import DatasetCollection
from schematools.exceptions import SchemaObjectNotFound

ST = TypeVar("ST", bound="SchemaType")
DTS = TypeVar("DTS", bound="DatasetTableSchema")
Json = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
Ref = str

logger = logging.getLogger(__name__)


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
        return f'SemVer("{str(self)}")'

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


@dataclass
class TableVersions:
    """Capture all active table definition versions.

    Upon reading in datasets
     :class:`TableVersions` nodes are explicitly inserted into the deserialized JSON schema
    to retain information about active table versions.
    This information would otherwise be lost
    by virtue of the `$ref` property being on the same level
    as the `activeVersions` property,
    and how `$ref`s are supposed to be treated.

    See Also: https://json-schema.org/understanding-json-schema/structuring.html#ref

    This a stop gap
    until the Amsterdam Meta Schema is extended to retain this information.
    """

    id: str  # noqa: A003
    """Table id."""

    default_version_number: SemVer
    """Version number of the default table version."""

    active: Dict[SemVer, Json]
    """All active table versions."""

    @property
    def default(self) -> Json:
        """Return default table version."""
        return self.active[self.default_version_number]


class TableVersionsEncoder(JSONEncoder):
    """Partially encode TableVersions to JSON.

    We allow for two different ways to define tables:

    1. Inline with the dataset definition.
       This allows for only one version of the table to be specified.
    2. In separate files referenced from the dataset definition.
       This allows for multiple versions of the table to be specified.

    Option 2. was originally introduced in a backwards compatible way;
    when the JSON definitions were loaded from either a path or URL,
    the node,
    in the JSON that referenced the tables,
    was replaced with the default version of the table.
    This allowed most of the existing code to keep working
    as if option 1 had been used.

    The downside of that approach is that specified information,
    namely that of active versions,
    is lost in the process.
    As a stop gap,
    this was temporarily remedied with the introduction of the :class:`TableVersions` object:
    it retained the references to all active versions on the table,
    including the default version,
    by enriching the deserialized dataset with additional information
    However,
    this cannot be serialized back to a single dataset definition
    with inline table definitions;
    the Amsterdam Meta Schema currently won't allow for it.

    This means that
    anything that expects a deserialized dataset
    that adheres to the Amsterdam Meta Schema,
    e.g. the DSO-API that wants to store the dataset definitions in SQL tables,
    should receive the original non-enriched JSON.
    That is exactly what this encoder does;
    it effectively removes everything that :class:`TableVersions` added.
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, TableVersions):
            return o.default
        return super().default(o)


class SchemaType(UserDict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key: str) -> NoReturn:
        raise KeyError(f"No field named '{key}' exists in {self!r}")

    def __hash__(self):
        return id(self)  # allow usage in lru_cache()

    @property
    def id(self) -> str:  # noqa: A003
        return cast(str, self["id"])

    @property
    def type(self) -> str:
        return cast(str, self["type"])

    def json(self) -> str:
        return json.dumps(self.data, cls=TableVersionsEncoder)

    def json_data(self) -> Json:
        return json.loads(self.json())

    @classmethod
    def from_dict(cls: Type[ST], obj: Json) -> ST:
        return cls(copy.deepcopy(obj))


class DatasetType(UserDict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key: str) -> NoReturn:
        raise KeyError(f"No field named '{key}' exists in {self!r}")


class DatasetSchema(SchemaType):
    """The schema of a dataset.

    This is a collection of JSON Schema's within a single file.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """When initializing a datasets, a cache of related datasets
        can be added (at classlevel). Thus, we are able to get (temporal) info
        about the related datasets
        """
        super().__init__(*args, **kwargs)
        self.dataset_collection = DatasetCollection()
        for i, table in enumerate(self["tables"]):
            if isinstance(table, TableVersions):
                continue
            try:
                dvn = SemVer(table["version"])
            except ValueError as e:
                raise ValueError(f"""{e} (in {table["id"]})""") from e
            self["tables"][i] = TableVersions(
                id=table["id"], default_version_number=dvn, active={dvn: table}
            )
        self.dataset_collection.add_dataset(self)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self['id']}>"

    @classmethod
    @deprecated(
        version="2.3.1",
        reason="""The `DatasetSchema.from_file` has been replaced by
            `schematools.utils.dataset_schema_from_path`.""",
    )
    def from_file(cls, filename: Union[Path, str]) -> DatasetSchema:
        """Open an Amsterdam schema from a file and any table files referenced therein"""
        from schematools.utils import dataset_schema_from_path

        return dataset_schema_from_path(filename)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> DatasetSchema:
        """Parses given dict and validates the given schema"""
        if obj.get("type") != "dataset" or not isinstance(obj.get("tables"), list):
            raise ValueError("Invalid Amsterdam Dataset schema file")

        return cls(copy.deepcopy(obj))

    @property
    def title(self) -> Optional[str]:
        """Title of the dataset (if set)"""
        return self.get("title")

    @property
    def description(self) -> Optional[str]:
        """Description of the dataset (if set)"""
        return self.get("description")

    @property
    def license(self) -> Optional[str]:
        """The license of the table as stated in the schema."""
        return self.get("license")

    @property
    def identifier(self) -> str:
        """Which fields acts as identifier. (default is Django "pk" field)"""
        return self.get("identifier", "pk")

    @property
    def version(self) -> str:
        """Dataset Schema Version"""
        return self.get("version", None)

    @property
    def default_version(self) -> str:
        """Default version for this schema"""
        return self.get("default_version", self.version)

    @property
    def is_default_version(self) -> bool:
        """Is this Default Dataset version.
        Defaults to True, in order to stay backwards compatible."""
        return self.default_version == self.version

    @property
    def auth(self) -> FrozenSet[str]:
        """Auth of the dataset (if set)"""
        return _normalize_scopes(self.get("auth"))

    def get_dataset_schema(self, dataset_id: str) -> DatasetSchema:
        return self.dataset_collection.get_dataset(dataset_id)

    @property
    def tables(self) -> List[DatasetTableSchema]:
        """Access the tables within the file"""
        tables: List[DatasetTableSchema] = []
        for tv in self["tables"]:
            if isinstance(tv, TableVersions):
                # Dataset was likely loaded using the path or URL loader that properly resolves
                # all the active tables. Hence the presence of the TableVersions node in
                # dictionary.
                tables.append(DatasetTableSchema(tv.default, parent_schema=self))
            else:
                # For backwards compatibility reasons assume the node in the dictionary is an
                # actual table definition. This happens when the schema is handed to us
                # by the DSO-API. See also :class:`TableVersionsEncoder` for a more complete rant.
                tables.append(DatasetTableSchema(tv, parent_schema=self))
        return tables

    def get_tables(
        self,
        include_nested: bool = False,
        include_through: bool = False,
    ) -> List[DatasetTableSchema]:
        """List tables, including nested"""
        tables = self.tables
        if include_nested:
            tables += self.nested_tables
        if include_through:
            tables += self.through_tables
        return tables

    @lru_cache()  # type: ignore[misc]
    def get_table_by_id(
        self, table_id: str, include_nested: bool = True, include_through: bool = True
    ) -> DatasetTableSchema:
        from schematools.utils import to_snake_case

        for table in self.get_tables(
            include_nested=include_nested, include_through=include_through
        ):
            if to_snake_case(table.id) == to_snake_case(table_id):
                return table

        available = "', '".join([table.default["id"] for table in self["tables"]])
        raise SchemaObjectNotFound(
            f"Table '{table_id}' does not exist "
            f"in schema '{self.id}', available are: '{available}'"
        )

    @property
    def nested_tables(self) -> List[DatasetTableSchema]:
        """Access list of nested tables."""
        return [
            self.build_nested_table(table=t, field=f)
            for t in self.tables
            for f in t.fields
            if f.is_nested_table
        ]

    @property
    def through_tables(self) -> List[DatasetTableSchema]:
        """Access list of through_tables, for n-m relations."""
        return [
            self.build_through_table(table=t, field=f)
            for t in self.tables
            for f in t.fields
            if f.is_through_table and not (f.is_loose_relation and f.nm_relation is None)
        ]

    def build_nested_table(
        self, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetTableSchema:
        # Map Arrays into tables.
        from schematools.utils import get_rel_table_identifier, to_snake_case

        def _get_parent_fk_type():
            """Get type of the parent identifier."""
            # composite keys are concatened to one id an thus always strings
            if len(table.identifier) > 1:
                return "string"
            return table.get_field_by_id(table.identifier[0]).type

        snakecased_field_id = to_snake_case(field.id)
        sub_table_id = get_rel_table_identifier(table.id, snakecased_field_id)

        sub_table_schema = {
            "id": sub_table_id,
            "originalID": field.id,
            "type": "table",
            "version": str(table.version),
            "auth": list(field.auth | table.auth),  # pass same auth rules as field has
            "description": f"Auto-generated table for nested field: {table.id}.{field.id}",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "schema"],
                "properties": {
                    "id": {"type": "integer/autoincrement", "description": ""},
                    "schema": {"$ref": "#/definitions/schema"},
                    "parent": {"type": _get_parent_fk_type(), "relation": f"{self.id}:{table.id}"},
                    **field["items"]["properties"],
                },
            },
        }

        # When shortnames are in use for table or field
        # we need to add a shortname to the dynamically generated
        # schema definition.
        if field.has_shortname or table.has_shortname:
            snakecased_fieldname: str = to_snake_case(field.name)
            sub_table_schema["shortname"] = get_rel_table_identifier(
                table.name, snakecased_fieldname
            )
        return DatasetTableSchema(
            sub_table_schema, parent_schema=self, _parent_table=table, nested_table=True
        )

    def build_through_table(
        self, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetTableSchema:
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
        from schematools.utils import get_rel_table_identifier, toCamelCase

        # Build the through_table for n-m relation
        # For relations, we have to use the real ids of the tables
        # and not the shortnames
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
        table_id = get_rel_table_identifier(table.id, target_field_id)

        sub_table_schema: Dict[str, Any] = {
            "id": table_id,
            "type": "table",
            "version": str(table.version),
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
            sub_table_schema["shortname"] = toCamelCase(f"{table.name}_{field.name}")

        # We also need to add a shortname for the individual FK fields
        # pointing to left en right table in the M2M
        if field.has_shortname:
            sub_table_schema["schema"]["properties"][target_field_id]["shortname"] = field.name
        if table.has_shortname:
            sub_table_schema["schema"]["properties"][left_table_id]["shortname"] = table.name

        # For both types of through tables (M2M and FK), we add extra fields
        # to the table (see docstring).
        if field.is_through_table:

            dim_fields = {}

            if field.is_object:
                properties = field.get("properties", {})
            elif field.is_array_of_objects:
                properties = field["items"].get("properties", {})
            else:
                properties = {}

            # Add the dimension fields, but only if those were defined in the
            # fields of the relation.
            for dim_field in field.get_dimension_fieldnames().get("geldigOp", []):
                if (camel_dim_field := toCamelCase(dim_field)) in properties:
                    dim_fields[camel_dim_field] = properties[camel_dim_field]

            right_table = self.dataset_collection.get_dataset(right_dataset_id).get_table_by_id(
                right_table_id, include_nested=False, include_through=False
            )
            for fk_target_table, relation_field_id in (
                (table, left_table_id),
                (right_table, target_field_id),
            ):
                if fk_target_table.has_composite_key and not field.is_loose_relation:
                    # Change the spec of a relation inside a schema to an object.
                    # Originally, the spec for the relation is based on singular key.
                    # When the relation has a composite key,
                    # the spec needs to be expanded into an object.
                    sub_table_schema = cast(Dict[str, Any], sub_table_schema)
                    spec = sub_table_schema["schema"]["properties"][relation_field_id]
                    spec["type"] = "object"
                    spec["properties"] = {
                        toCamelCase(idf): {"type": fk_target_table.get_field_by_id(idf).type}
                        for idf in fk_target_table.identifier
                    }

            sub_table_schema["schema"]["properties"].update(dim_fields)

        return DatasetTableSchema(
            sub_table_schema, parent_schema=self, _parent_table=table, through_table=True
        )

    @property
    def related_dataset_schema_ids(self) -> Set[str]:
        """Fetch a list or related schema ids.

        When a dataset has relations,
        it needs to build up tables on the fly with the information
        in the associated table. This property calculates the dataset_schema_ids
        that are needed, so the users of this dataset can preload these
        datasets.

        We also collect the FK relation that possibly do not have temporal
        characteristics. However, we cannot know this for sure if not also the
        target dataset of a relation has been loaded.
        """
        related_ids = set()
        for table in self.tables:
            for f in table.get_fields(include_subfields=False):
                a_relation = f.relation or f.nm_relation
                if a_relation is not None:
                    dataset_id, _ = a_relation.split(":")
                    related_ids.add(dataset_id)
        return related_ids


class DatasetTableSchema(SchemaType):
    """The table within a dataset.
    This table definition follows the JSON Schema spec.

    This class has an `id` property (inherited from `SchemaType`) to uniquely
    address this dataset-table in the scope of the `DatasetSchema`.
    This `id` is used in lots of places in the dynamic model generation in Django.

    There is also a `name` attribute, that is used for the autogeneration
    of tablenames that are used in postgreSQL.

    This `name` attribute is equal to the `id`, unless there is a `shortname`
    defined. In that case `name` is equal to the `shortname`.

    The `shortname` has been added for practical purposes, because there is a hard
    limitation on the length of tablenames in databases like postgreSQL.
    """

    def __init__(
        self,
        *args: Any,
        parent_schema: DatasetSchema,
        _parent_table: Optional[DatasetTableSchema] = None,
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
            raise ValueError("Invalid Amsterdam schema table data")

        if not self["schema"].get("$schema", "").startswith("http://json-schema.org/"):
            raise ValueError("Invalid JSON-schema contents of table")

    def __repr__(self):
        prefix = ""
        if self._parent_schema is not None:
            prefix += f"{self._parent_schema.id}."
        if self._parent_table is not None:
            prefix += f"{self._parent_table.id}."
        return f"<{self.__class__.__name__}: {prefix}{self['id']}>"

    @property
    def name(self) -> str:
        return self.get("shortname", self.id)

    @property
    def title(self) -> Optional[str]:
        """Title of the table."""
        return self.get("title")

    @property
    def has_shortname(self) -> bool:
        return self.get("shortname") is not None

    @property
    def dataset(self) -> DatasetSchema:
        """The dataset that this table is part of."""
        return self._parent_schema

    @property
    def parent_table(self) -> Optional[DatasetTableSchema]:
        """The parent table of this table.

        For nested and through tables, the parent table is available.
        """
        return self._parent_table

    @property
    def parent_table_field(self) -> Optional[DatasetFieldSchema]:
        """Provide the NM-relation that generated this through table."""
        if self.through_table or self.nested_table:
            return self._parent_table.get_field_by_id(self["originalID"])
        else:
            return None

    @property
    def description(self) -> Optional[str]:
        """The description of the table as stated in the schema."""
        return self.get("description")

    def get_fields(self, include_subfields: bool = False) -> Iterator[DatasetFieldSchema]:
        """Get the fields for this table.

        Args:
            include_subfields: Merge the subfields of an FK relation into the fields
            of this table. The ids of these fields need to be prefixed
            (usually with the `id` of the relation field) to avoid name collisions.
        """
        required = set(self["schema"]["required"])
        for id_, spec in self["schema"]["properties"].items():
            field_schema = DatasetFieldSchema(
                _parent_table=self,
                _required=(id_ in required),
                **{**spec, "id": id_},
            )

            # Add extra fields for relations of type object
            # These fields are added to identify the different
            # components of a composite FK to a another table
            if field_schema.relation is not None and field_schema.is_object and include_subfields:
                for subfield in field_schema.get_subfields(add_prefixes=True):
                    # We exclude temporal fields, they need not to be merged into the table fields
                    if subfield.is_temporal:
                        continue
                    yield subfield
            yield field_schema

        # If composite key, add PK field
        if self.has_composite_key and "id" not in self["schema"]["properties"]:
            yield DatasetFieldSchema(_parent_table=self, _required=True, type="string", id="id")

    @cached_property
    def fields(self) -> List[DatasetFieldSchema]:
        # TODO: this should not return sub fields!
        return list(self.get_fields(include_subfields=True))

    @lru_cache()  # type: ignore[misc]
    def get_fields_by_id(self, *field_ids: str) -> List[DatasetFieldSchema]:
        """Get the fields based on the ids of the fields.

        args:
            field_ids: The ids of the fields.
            NB. This needs to be a tuple, lru_cache only works on immutable arguments.
        """
        field_ids_set: Set[str] = set(field_ids)
        return [field for field in self.fields if field.id in field_ids_set]

    @lru_cache()  # type: ignore[misc]
    def get_field_by_id(self, field_id: str) -> DatasetFieldSchema:
        """Get a fields based on the ids of the field."""
        for field_schema in self.fields:
            if field_schema.id == field_id:
                return field_schema

        raise SchemaObjectNotFound(f"Field '{field_id}' does not exist in table '{self.id}'.")

    def get_through_tables_by_id(self) -> List[DatasetTableSchema]:
        """Access list of through_tables (for n-m relations) for a single base table."""
        if self.dataset is None:
            return []
        return [
            self.dataset.build_through_table(table=self, field=f)
            for f in self.fields
            if f.is_through_table and not (f.is_loose_relation and f.relation is None)
        ]

    @property
    def display_field(self) -> Optional[str]:
        """Tell which fields can be used as display field."""
        return cast(Optional[str], self["schema"].get("display"))

    def get_dataset_schema(self, dataset_id: str) -> Optional[DatasetSchema]:
        """Return the associated parent datasetschema for this table."""
        return self.dataset.get_dataset_schema(dataset_id) if self.dataset is not None else None

    @cached_property
    def temporal(self) -> Optional[Temporal]:
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

        return Temporal(
            identifier=identifier,
            dimensions={
                key: TemporalDimensionFields(start_field, end_field)
                for key, [start_field, end_field] in dimensions.items()
            },
        )

    @property
    def is_temporal(self) -> bool:
        """Indicates if this is a table with temporal characteristics"""
        return "temporal" in self

    @property
    def main_geometry(self) -> str:
        """The main geometry field, if there is a geometry field available.
        Default to "geometry" for existing schemas without a mainGeometry field.
        """
        return str(self["schema"].get("mainGeometry", "geometry"))

    @property
    def identifier(self) -> List[str]:
        """The main identifier field, if there is an identifier field available.
        Default to "id" for existing schemas without an identifier field.
        """
        identifier = self["schema"].get("identifier", ["id"])
        # Convert identifier to a list, to be backwards compatible with older schemas
        if not isinstance(identifier, list):
            identifier = [identifier]
        return cast(list, identifier)  # mypy pleaser

    @property
    def is_autoincrement(self) -> bool:
        """Return bool indicating autoincrement behaviour of the table identifier."""
        if self.has_composite_key:
            return False
        return self.get_field_by_id(first(self.identifier)).is_autoincrement

    @property
    def has_composite_key(self) -> bool:
        """Tell whether the table uses multiple attributes together as it's identifier."""
        # Mypy bug that has been resolved but not merged
        # https://github.com/python/mypy/issues/9907
        if isinstance(self.identifier, str):  # type: ignore[unreachable]
            return False  # type: ignore[unreachable]
        return len(self.identifier) > 1

    def validate(self, row: Json) -> None:
        """Validate a record against the schema."""
        jsonschema.validate(row, self.data["schema"], format_checker=draft7_format_checker)

    def _resolve(self, ref: str) -> jsonschema.RefResolver:
        """Resolve the actual data type of a remote URI reference."""
        return jsonschema.RefResolver(ref, referrer=self)

    @property
    def has_parent_table(self) -> bool:
        """For nested or through tables, there is a parent table."""
        return self.nested_table or self.through_table

    @property
    @deprecated("additionalFilters is no longer supported")
    def filters(self):
        return dict(self["schema"].get("additionalFilters", {}))

    @property
    def relations(self):
        warnings.warn(
            "Using DatasetTableSchema.relations is deprecated, use additional_relations instead.",
            DeprecationWarning,
        )
        return dict(self["schema"].get("additionalRelations", {}))

    @property
    @deprecated("additionalFilters is no longer supported")
    def additional_filters(self) -> Dict[str, Dict[str, str]]:
        """Fetch list of additional filters.
        Example value:

            "regimes.inWerkingOp": {
              "type": "range",
              "start": "regimes.beginTijd",
              "end": "regimes.eindTijd"
            }
        """
        return dict(self["schema"].get("additionalFilters", {}))

    @cached_property
    def additional_relations(self) -> List[AdditionalRelationSchema]:
        """Fetch list of additional (backwards or N-N) relations.

        This is a dictionary of names for existing forward relations
        in other tables with either the 'embedded' or 'summary'
        property
        """
        return [
            AdditionalRelationSchema(name, self, **relation)
            for name, relation in self["schema"].get("additionalRelations", {}).items()
        ]

    def get_reverse_relation(
        self, field: DatasetFieldSchema
    ) -> Optional[AdditionalRelationSchema]:
        """Find the description of a reverse relation for a field."""
        if not field.relation and not field.nm_relation:
            raise ValueError("Field is not a relation")

        for relation in self.additional_relations:
            if relation.is_reverse_relation(field):
                return relation

        return None

    @property
    def auth(self) -> FrozenSet[str]:
        """Auth of the table (if set)"""
        return _normalize_scopes(self.get("auth"))

    @property
    def is_through_table(self) -> bool:
        """Indicate if table is an intersection table (n:m relation table) or base table."""
        return self.through_table

    @property
    def is_nested_table(self) -> bool:
        """Indicates if table is an nested table"""
        return self.nested_table

    def model_name(self) -> str:
        """Returns model name for this table. Including version number, if needed."""

        from schematools.utils import to_snake_case

        if self.dataset is None:
            raise ValueError(
                "Cannot obtain a model_name from a DatasetTableSchema without a parent dataset."
            )
        model_name = self.id
        if self.dataset.version is not None and not self.dataset.is_default_version:
            model_name = f"{model_name}_{self.dataset.version}"
        return to_snake_case(model_name)

    def db_name(
        self,
        *,
        with_dataset_prefix: bool = True,
        with_version: bool = False,
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
        from schematools.utils import to_snake_case

        dataset_prefix = version_postfix = ""
        if with_version:
            version_postfix = self.version.signif
        if with_dataset_prefix:
            dataset_prefix = to_snake_case(self.dataset.id)
        if self.nested_table or self.through_table:
            # We don't automatically shorten user defined table names. Automatically generated
            # names, however, should be shortened as the user has no direct control over them.
            db_table_name = "_".join(filter(None, (dataset_prefix, to_snake_case(self.name))))
            additional_underscores = len(list(filter(None, (version_postfix, postfix))))
            max_length = (
                MAX_TABLE_NAME_LENGTH
                - len(version_postfix)
                - len(postfix)
                - additional_underscores
            )
            # Shortening should preserve both postfixes
            db_table_name = (
                "_".join(filter(None, (db_table_name[:max_length], version_postfix))) + postfix
            )
        else:
            # User defined table name -> no shortening
            db_table_name = (
                "_".join(filter(None, (dataset_prefix, to_snake_case(self.name), version_postfix)))
                + postfix
            )
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
        if check_assert:
            assert len(db_table_name) <= MAX_TABLE_NAME_LENGTH, (
                f"table name {db_table_name!r} is too long, having {len(db_table_name)} chars. "
                f"Max allowed length is {MAX_TABLE_NAME_LENGTH} chars."
            )
        return db_table_name

    def get_fk_fields(self) -> Iterator[str]:
        """Generates fields names that contain a 1:N relation to a parent table"""
        fields_items = self["schema"]["properties"].items()
        field_schema = (
            DatasetFieldSchema(_parent_table=self, **{**spec, "id": _id})
            for _id, spec in fields_items
        )
        return (f.name for f in field_schema if f.relation)

    @property
    def version(self) -> SemVer:
        """Get table version."""
        # It's a required attribute, hence should be present.
        return SemVer(self["version"])

    @classmethod
    def from_dict(cls: Type[DTS], obj: Json) -> DTS:  # noqa: D102
        raise Exception(
            f"A dict is not sufficient anymore to instantiate a {cls.__name__!r}. "
            "Use regular class instantiation instead and supply all required parameters!"
        )


class DatasetFieldSchema(DatasetType):
    """A single field (column) in a table."""

    def __init__(
        self,
        *args: Any,
        _parent_table: Optional[DatasetTableSchema],
        _parent_field: Optional[DatasetFieldSchema] = None,
        _required: bool = False,
        _temporal: bool = False,
        **kwargs: Any,
    ) -> None:
        self._id: str = kwargs.pop("id")
        super().__init__(*args, **kwargs)
        self._parent_table = _parent_table
        self._parent_field = _parent_field
        self._required = _required
        self._temporal = _temporal

    def __repr__(self) -> str:
        prefix = ""
        if self._parent_table is not None:
            prefix = f"{self._parent_table.id}."
        if self._parent_field is not None:
            prefix += f"{self._parent_field.id}."
        return f"<{self.__class__.__name__}: {prefix}{self._id}>"

    @property
    def table(self) -> Optional[DatasetTableSchema]:
        """The table that this field is a part of"""
        return self._parent_table

    @property
    def parent_field(self) -> Optional[DatasetFieldSchema]:
        """Provide access to the top-level field where it is a property for."""
        return self._parent_field

    @property
    def id(self) -> str:
        """The id of a field uniquely identifies it among the fields of a table."""
        return self._id

    @property
    def is_autoincrement(self) -> bool:
        return "autoincrement" in self.type

    @property
    def name(self) -> str:
        """
        The name of a field is used to derive its column name in SQL.

        The name is equal to the id, except when it is overridden by the presence
        of a shortname.

        The actual column name in SQL is the snake-casing of the name.
        """
        return cast(str, self.get("shortname", self._id))

    @property
    def title(self) -> Optional[str]:
        """Title of the field."""
        return self.get("title")

    @property
    def has_shortname(self) -> bool:
        """Reports whether this field has a shortname.

        You should never have to call this: name returns the shortname, if present.
        """
        return self.get("shortname") is not None

    @property
    def description(self) -> Optional[str]:
        return self.get("description")

    @property
    def required(self) -> bool:
        return self._required

    @property
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

    @property
    def is_primary(self) -> bool:
        """When name is 'id' the field should be the primary key
        For composite keys (table.identifier has > 1 item), an 'id'
        field is autogenerated.
        """
        if self.table is None:
            return False
        return self.name == "id" or [self.name] == self.table.identifier

    @property
    def relation(self) -> Optional[str]:
        """Give the 1:N relation, if it exists."""
        if self.type == "array":
            return None
        return self.get("relation")

    @property
    def nm_relation(self) -> Optional[str]:
        """Give the N:M relation, if it exists (called M2M in Django)."""
        if self.type != "array":
            return None
        return self.get("relation")

    @cached_property
    def related_table(self) -> Optional[DatasetTableSchema]:
        """If this field is a relation, return the table this relation references."""
        relation = self.get("relation")  # works for both 1:N and N:M relations
        if not relation:
            return None

        # Find the related field
        related_dataset_id, related_table_id = relation.split(":")
        dataset = cast(DatasetSchema, self.table.dataset)
        dataset = dataset.dataset_collection.get_dataset(related_dataset_id)
        return dataset.get_table_by_id(
            related_table_id, include_nested=False, include_through=False
        )

    @property
    def related_field_ids(self) -> Optional[list[str]]:
        """If this field is a relation, return which fields this relation references.
        That can be either the primary key of the related table,
        or one of the explicitly declared sub-fields.
        """
        if not self.get("relation"):
            return None
        elif self.is_object:
            # Relation where the fields are defined as sub-fields
            return list(self["properties"].keys())
        else:
            # References the primary key of the related table.
            return self.related_table.identifier

    @property
    def reverse_relation(self) -> Optional[AdditionalRelationSchema]:
        """Find the opposite description of a relation.

        When there is a relation, this only returns a description
        when the linked table also describes the other end of relationship.
        """
        related_table = self.related_table
        if related_table is None:
            return None

        return related_table.get_reverse_relation(self)

    @property
    def format(self) -> Optional[str]:
        return self.get("format")

    @property
    def multipleof(self) -> Optional[float]:
        return self.get("multipleOf")

    @property
    def is_object(self) -> bool:
        """Tell whether the field references an object."""
        return self.get("type") == "object"

    @property
    def is_scalar(self) -> bool:
        """Tell whether the field is a scalar."""
        return self.get("type") not in {"object", "array"}

    @property
    def is_temporal(self) -> bool:
        """Tell whether the field is added, because it has temporal charateristics"""
        return self._temporal

    @property
    def is_geo(self) -> bool:
        """Tell whether the field references a geo object."""
        return "geojson.org" in self.get("$ref", "")

    @property
    def provenance(self) -> Optional[str]:
        """Get the provenance info, if available, or None"""
        return self.get("provenance")

    @property
    def field_items(self) -> Optional[Json]:
        """Return the item definition for an array type."""
        return self.get("items", {}) if self.is_array else None

    def get_dimension_fieldnames(self) -> Dict[str, TemporalDimensionFields]:
        """Gets the dimension fieldnames."""
        if self.relation is None and self.nm_relation is None:
            return {}

        dataset_id, table_id = cast(str, self.relation or self.nm_relation).split(":")
        if self.table is None:
            return {}

        dataset_schema = self.table.get_dataset_schema(dataset_id)
        if dataset_schema is None:
            return {}
        try:
            dataset_table = dataset_schema.get_table_by_id(
                table_id, include_nested=False, include_through=False
            )
        except ValueError:
            # If we cannot get the table, we ignore the exception
            # and we do not return fields
            return {}
        if not dataset_table.is_temporal:
            return {}

        return dataset_table.temporal.dimensions

    @lru_cache()  # type: ignore[misc]
    def get_field_by_id(self, field_id: str) -> DatasetFieldSchema:
        """Finds and returns the subfield with the given id.

        SchemaObjectNotFound is raised when the field does not exist.
        """
        for field_schema in self.subfields:
            if field_schema.id == field_id:
                return field_schema

        name = self.table.id + "." + self.id
        raise SchemaObjectNotFound(f"Subfield '{field_id}' does not exist below field '{name}'.")

    @cached_property
    def subfields(self) -> List[DatasetFieldSchema]:
        """Return the subfields for a nested structure.

        Calls the `get_subfields` method without argument,
        so no prefixes are added to the field ids.
        This is the default situation.
        """
        return list(self.get_subfields())

    def get_subfields(self, add_prefixes: bool = False) -> Iterable[DatasetFieldSchema]:
        """Return the subfields for a nested structure.

        Args:
            add_prefixes: Add prefixes to the ids of the subfields.

        For a nested object, fields are based on its properties,
        for an array of objects, fields are based on the properties
        of the "items" field.

        When subfields are added as part of an 1m-relation
        those subfields need to be prefixed with the name of the relation field.
        However, this is not the case for the so-called `dimension` fields
        of a temporal relation (e.g. `beginGeldigheid` and `eindGeldigheid`).

        If self is not an object or array, the return value is an empty iterator.
        """
        from schematools.utils import toCamelCase

        field_name_prefix = ""

        if self.is_object:
            # Field has direct subfields (type=object)
            required = set(self.get("required", []))
            properties = self["properties"]
        elif self.is_array_of_objects and self.field_items is not None:
            # Field has an array of objects (type=array, items are objects)
            required = set(self.field_items.get("required") or ())
            properties = self.field_items["properties"]
        else:
            return ()

        relation = self.relation
        nm_relation = self.nm_relation
        if relation is not None or nm_relation is not None:
            field_name_prefix = self.name + RELATION_INDICATOR

        combined_dimension_fieldnames: Set[str] = set()
        for (_dimension, field_names) in self.get_dimension_fieldnames().items():
            combined_dimension_fieldnames |= {toCamelCase(fieldname) for fieldname in field_names}

        for id_, spec in properties.items():
            needs_prefix = add_prefixes and id_ not in combined_dimension_fieldnames
            field_id = f"{field_name_prefix}{id_}" if needs_prefix else id_
            yield DatasetFieldSchema(
                _parent_table=self._parent_table,
                _parent_field=self,
                _required=(id_ in required),
                _temporal=(id_ in combined_dimension_fieldnames),
                **{**spec, "id": field_id},
            )

    @property
    def is_array(self) -> bool:
        """
        Checks if field is an array field
        """
        return self.get("type") == "array"

    @property
    def is_array_of_objects(self) -> bool:
        """
        Checks if field is an array of objects
        """
        return self.is_array and self.get("items", {}).get("type") == "object"

    @property
    def is_array_of_scalars(self) -> bool:
        """
        Checks if field is an array of scalars
        """
        return self.is_array and self.get("items", {}).get("type") != "object"

    @property
    def is_nested_table(self) -> bool:
        """
        Checks if field is a possible nested table.
        """
        return self.is_array_of_objects and self.nm_relation is None

    @property
    def is_through_table(self) -> bool:
        """
        Checks if field is a possible through table.

        NM tables always are through tables. For 1N tables, there is a through
        tables if the target of the relation is temporal.
        """
        return self.nm_relation is not None or self.is_relation_temporal

    @property
    def is_relation_temporal(self):
        """Tell whether the 1-N relationship is modelled by an intermediate table.
        This allows tracking multiple versions of the relationship.
        """
        return self.relation is not None and self.related_table.is_temporal

    @property
    def auth(self) -> FrozenSet[str]:
        """Auth of the field, or the empty set if auth is not set."""
        return _normalize_scopes(self.get("auth"))

    @property
    def is_composite_key(self):
        """Tell whether the relation uses a composite key"""
        return self.get("relation") and self.is_object and len(self["properties"]) > 1

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
        identifier_field = related_table.get_field_by_id(related_table.identifier[0])
        sequence_field = related_table.get_field_by_id(related_table.temporal.identifier)

        if self.is_array_of_objects:
            properties = self.field_items["properties"]
        elif self.is_object:
            properties = self["properties"]
        else:
            raise ValueError("Relations should have string/array/object type")

        source_type_set = {
            (prop_name, prop_val["type"]) for prop_name, prop_val in properties.items()
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


class DatasetRow(DatasetType):
    """An actual instance of data"""

    def validate(self, schema: DatasetSchema) -> None:
        table = schema.get_table_by_id(self["table"])
        table.validate(self.data)


class AdditionalRelationSchema(DatasetType):
    """Data class describing the additional relation block"""

    def __init__(self, _id: str, _parent_table: Optional[DatasetTableSchema] = None, **kwargs):
        super().__init__(**kwargs)
        self._id = _id
        self._parent_table = _parent_table

    @property
    def id(self):
        return self._id

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
        """Provide the relation identifier"""
        # Currently generated, will change in schema later
        return f"{self._parent_table.dataset.id}:{self['table']}:{self['field']}"

    @cached_property
    def related_table(self) -> DatasetTableSchema:
        """Return the table this relation references."""
        # NOTE: currently doesn't cross datasets
        return self._parent_table.dataset.get_table_by_id(
            self["table"], include_nested=False, include_through=False
        )

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
    def from_string(cls, value: Optional[str]) -> PermissionLevel:
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


@dataclass(order=True)
class Permission:
    """The result of an authorisation check.
    The extra fields in this dataclass are mainly provided for debugging purposes.
    The dataclass can also be ordered; they get sorted by access level.
    """

    #: The permission level given by the profile
    level: PermissionLevel

    #: The extra parameter for the level (e.g. "letters:3")
    sub_value: Optional[str] = None

    #: Who authenticated this (added for easier debugging. typically tested against)
    source: Optional[str] = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if self.level is PermissionLevel.NONE:
            # since profiles only grant permission,
            # having no permission is always from the schema.
            self.source = "schema"

    @classmethod
    def from_string(cls, value: Optional[str], source: Optional[str] = None) -> Permission:
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

    def transform_function(self) -> Optional[Callable[[Json], Json]]:
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
    def name(self) -> Optional[str]:
        """Name of Profile (if set)"""
        return self.get("name")

    @property
    def scopes(self) -> FrozenSet[str]:
        """All these scopes should match in order to activate the profile."""
        return _normalize_scopes(self.get("scopes"))

    @cached_property
    def datasets(self) -> Dict[str, ProfileDatasetSchema]:
        """The datasets that this profile provides additional access rules for."""
        return {
            id: ProfileDatasetSchema(id, self, data)
            for id, data in self.get("datasets", {}).items()
        }


class ProfileDatasetSchema(DatasetType):
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
    def profile(self) -> Optional[ProfileSchema]:
        """The profile that this definition is part of."""
        return self._parent_schema

    @cached_property
    def permissions(self) -> Permission:
        """Global permissions that are granted to the dataset. e.g. "read"."""
        return Permission.from_string(
            self.get("permissions"), source=f"profiles[{self._id}].dataset"
        )

    @cached_property
    def tables(self) -> Dict[str, ProfileTableSchema]:
        """The tables that this profile provides additional access rules for."""
        return {
            id: ProfileTableSchema(id, self, data) for id, data in self.get("tables", {}).items()
        }


class ProfileTableSchema(DatasetType):
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
    def dataset(self) -> Optional[ProfileDatasetSchema]:
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
    def fields(self) -> Dict[str, Permission]:
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
    def mandatory_filtersets(self) -> List[List[str]]:
        """Tell whether the listing can only be requested with certain inputs.
        E.g. an API user may only list data when they supply the lastname + birthdate.

        Example value::

            [
              ["bsn", "lastname"],
              ["postcode", "regimes.aantal[gte]"]
            ]
        """
        return self.get("mandatoryFilterSets", [])


class TemporalDimensionFields(NamedTuple):
    """A tuple that describes the start field and end field of a range.
    This could be something like ``("beginGeldigheid", "eindGeldigheid")``.
    """

    start: str
    end: str


@dataclass
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
    dimensions: Dict[str, TemporalDimensionFields] = field(default_factory=dict)


def _normalize_scopes(auth: Union[None, str, list, tuple]) -> FrozenSet[str]:
    """Make sure the auth field has a consistent type"""
    if not auth:
        # Auth defined on schema
        return frozenset()
    elif isinstance(auth, (list, tuple, set)):
        # Multiple scopes act choices (OR match).
        return frozenset(auth)
    else:
        # Normalize single scope to set return type too.
        return frozenset({auth})
