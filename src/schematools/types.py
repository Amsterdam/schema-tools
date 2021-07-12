"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import json
from collections import UserDict
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property, total_ordering
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    NoReturn,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import jsonschema
from methodtools import lru_cache

from schematools import RELATION_INDICATOR
from schematools.datasetcollection import DatasetCollection
from schematools.exceptions import SchemaObjectNotFound

if TYPE_CHECKING:
    from schematools.permissions import UserScopes

ST = TypeVar("ST", bound="SchemaType")
Json = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class SchemaType(UserDict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key: str) -> NoReturn:
        raise KeyError(f"No field named '{key}' exists in {self!r}")

    def __hash__(self):
        return id(self)  # allow usage in lru_cache()

    @property
    def id(self) -> str:
        return cast(str, self["id"])

    @property
    def type(self) -> str:
        return cast(str, self["type"])

    def json(self) -> str:
        return json.dumps(self.data)

    def json_data(self) -> Json:
        return self.data

    @classmethod
    def from_dict(cls: Type[ST], obj: Json) -> ST:
        return cls(obj)


class DatasetType(UserDict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key: str) -> NoReturn:
        raise KeyError(f"No field named '{key}' exists in {self!r}")


class DatasetSchema(SchemaType):
    """The schema of a dataset.

    This is a collection of JSON Schema's within a single file.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """When initializing a datasets, a cache of related datasets
        can be added (at classlevel). Thus, we are able to get (temporal) info
        about the related datasets
        """
        super().__init__(*args, **kwargs)
        self.dataset_collection = DatasetCollection()
        self.dataset_collection.add_dataset(self)

    @classmethod
    def from_file(cls, filename: str) -> DatasetSchema:
        """Open an Amsterdam schema from a file."""
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Json) -> DatasetSchema:
        """Parses given dict and validates the given schema"""
        if obj.get("type") != "dataset" or not isinstance(obj.get("tables"), list):
            raise ValueError("Invalid Amsterdam Schema file")

        return cls(obj)

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
        return [DatasetTableSchema(i, _parent_schema=self) for i in self["tables"]]

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

        available = "', '".join([table["id"] for table in self["tables"]])
        raise SchemaObjectNotFound(
            f"Table '{table_id}' does not exist "
            f"in schema '{self.id}', available are: '{available}'"
        )

    @property
    def nested_tables(self) -> List[DatasetTableSchema]:
        """Access list of nested tables."""
        tables = []
        for table in self.tables:
            for field in table.fields:
                if field.is_nested_table:
                    tables.append(self.build_nested_table(table=table, field=field))
        return tables

    @property
    def through_tables(self) -> List[DatasetTableSchema]:
        """Access list of through_tables, for n-m relations."""
        tables = []
        for table in self.tables:
            for field in table.fields:
                if field.is_through_table:
                    tables.append(self.build_through_table(table=table, field=field))
        return tables

    def build_nested_table(
        self, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetTableSchema:
        # Map Arrays into tables.
        from schematools.utils import get_rel_table_identifier, to_snake_case

        snakecased_field_id = to_snake_case(field.id)
        sub_table_id = get_rel_table_identifier(len(self.id) + 1, table.id, snakecased_field_id)
        sub_table_schema = {
            "id": sub_table_id,
            "originalID": field.name,
            "parentTableID": table.id,
            "type": "table",
            "auth": list(field.auth | table.auth),  # pass same auth rules as field has
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "schema"],
                "properties": {
                    "id": {"type": "integer/autoincrement", "description": ""},
                    "schema": {"$ref": "#/definitions/schema"},
                    "parent": {"type": "integer", "relation": f"{self.id}:{table.id}"},
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
                len(self.id) + 1, table.name, snakecased_fieldname
            )
        return DatasetTableSchema(sub_table_schema, _parent_schema=self, nested_table=True)

    def build_through_table(
        self, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetTableSchema:
        """Build the through table.

        The through tables are not defined separately in a schema.
        The fact that a M2M relation needs an extra table is an implementation aspect.
        However, the through (aka. junction) table schema is needed for the
        dyanamic model generation and for data-importing.

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
        from schematools.utils import get_rel_table_identifier, to_snake_case, toCamelCase

        # Build the through_table for n-m relation
        # For relations, we have to use the real ids of the tables
        # and not the shortnames
        left_dataset_id = to_snake_case(self.id)
        left_table_id = to_snake_case(table.id)
        left_table_name = to_snake_case(table.name)

        # Both relation types can have a through table,
        # For FK relations, an extra through_table is created when
        # the table is temporal, to store the extra temporal information.
        relation = field.nm_relation
        if relation is None and table.is_temporal:
            relation = field.relation

        # If the field is not a relation, that is an error and should not happen, bail out!
        if relation is None:
            raise ValueError(f"Field {field.id} for table {table.id} should be a relation!")

        right_dataset_id, right_table_id = [
            to_snake_case(part) for part in str(relation).split(":")[:2]
        ]

        # XXX maybe not logical to snakecase the fieldname here.
        # this is still schema-land.
        snakecased_fieldname = to_snake_case(field.name)
        snakecased_field_id = to_snake_case(field.id)
        table_id = get_rel_table_identifier(len(self.id) + 1, table.id, snakecased_field_id)

        sub_table_schema: Json = {
            "id": table_id,
            "type": "table",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["schema"],
                "properties": {
                    "schema": {"$ref": "#/definitions/schema"},
                    left_table_name: {
                        "type": "string",
                        "relation": f"{left_dataset_id}:{left_table_id}",
                    },
                    snakecased_fieldname: {
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
            sub_table_schema["shortname"] = get_rel_table_identifier(
                len(self.id) + 1, table.name, snakecased_fieldname
            )

        # Get the schema of the target table, to be able to get the
        # identifier fields.
        target_identifier_fields: Set[str] = set()
        if table.dataset is not None:
            target_table = table.dataset.get_dataset_schema(right_dataset_id).get_table_by_id(
                right_table_id, include_nested=False, include_through=False
            )
            target_identifier_fields = set(target_table.identifier)

        # For both types of through tables (M2M and FK), we add extra fields
        # to the table (see docstring).
        if field.is_through_table and target_identifier_fields:
            if field.is_object:
                properties = field.get("properties", {})
            elif field.is_array_of_objects:
                properties = field["items"].get("properties", {})
            else:
                properties = {}
            # Prefix the fields for the target side of the relation
            extra_fields = {}
            for sub_field_id, sub_field in properties.items():
                # if source table has shortname, add shortname
                if field.has_shortname:
                    sub_field["shortname"] = toCamelCase(f"{field.name}_{sub_field_id}")
                if sub_field_id in target_identifier_fields:
                    sub_field_id = toCamelCase(f"{field.id}_{sub_field_id}")
                extra_fields[sub_field_id] = sub_field

            # Also add the fields for the source side of the relation
            if table.has_compound_key:
                for sub_field_schema in table.get_fields_by_id(*table.identifier):
                    sub_field_id = toCamelCase(f"{table.id}_{sub_field_schema.id}")
                    extra_fields[sub_field_id] = sub_field_schema.data

            sub_table_schema["schema"]["properties"].update(extra_fields)

        return DatasetTableSchema(sub_table_schema, _parent_schema=self, through_table=True)

    @property
    def related_dataset_schema_ids(self) -> Iterator[str]:
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
        for table in self.tables:
            for field in table.get_fields(include_sub_fields=False):
                a_relation = field.relation or field.nm_relation
                if a_relation is not None:
                    dataset_id, table_id = a_relation.split(":")
                    yield dataset_id


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
        _parent_schema: Optional[DatasetSchema] = None,
        nested_table: bool = False,
        through_table: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._parent_schema = _parent_schema
        self.nested_table = nested_table
        self.through_table = through_table

        if self.get("type") != "table":
            raise ValueError("Invalid Amsterdam schema table data")

        if not self["schema"].get("$schema", "").startswith("http://json-schema.org/"):
            raise ValueError("Invalid JSON-schema contents of table")

    @property
    def name(self) -> str:
        return self.get("shortname", self.id)

    @property
    def has_shortname(self) -> bool:
        return self.get("shortname") is not None

    @property
    def dataset(self) -> Optional[DatasetSchema]:
        """The dataset that this table is part of."""
        return self._parent_schema

    @property
    def description(self) -> Optional[str]:
        """The description of the table as stated in the schema."""
        return self.get("description")

    def get_fields(self, include_sub_fields: bool = False) -> Iterator[DatasetFieldSchema]:
        required = set(self["schema"]["required"])
        for id_, spec in self["schema"]["properties"].items():
            field_schema = DatasetFieldSchema(
                _parent_table=self,
                _required=(id_ in required),
                **{**spec, "id": id_},
            )

            # Add extra fields for relations of type object
            # These fields are added to identify the different
            # components of a compound FK to a another table
            if field_schema.relation is not None and field_schema.is_object and include_sub_fields:
                for subfield_schema in field_schema.sub_fields:
                    yield subfield_schema
            yield field_schema

        # If compound key, add PK field
        # XXX we should check for an existing "id" field, avoid collisions
        if self.has_compound_key:
            yield DatasetFieldSchema(_parent_table=self, _required=True, type="string", id="id")

    @property
    def fields(self) -> Iterator[DatasetFieldSchema]:
        yield from self.get_fields(include_sub_fields=True)

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
        tables = []
        if self.dataset is None:
            return []
        for field in self.fields:
            if field.is_through_table:
                tables.append(self.dataset.build_through_table(table=self, field=field))
        return tables

    @property
    def display_field(self) -> Optional[str]:
        """Tell which fields can be used as display field."""
        return cast(Optional[str], self["schema"].get("display"))

    def get_dataset_schema(self, dataset_id: str) -> Optional[DatasetSchema]:
        """Return the associated parent datasetschema for this table."""
        return self.dataset.get_dataset_schema(dataset_id) if self.dataset is not None else None

    @property
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

        from schematools.utils import to_snake_case

        if temporal_config.get("identifier") is None or temporal_config.get("dimensions") is None:
            raise ValueError("Invalid temporal data")

        dimensions: Dict[str, Tuple[str, str]] = {}
        for key, [start_field, end_field] in temporal_config.get("dimensions").items():
            dimensions[key] = (
                to_snake_case(start_field),
                to_snake_case(end_field),
            )

        return Temporal(temporal_config.get("identifier"), dimensions)

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
    def has_compound_key(self) -> bool:
        # Mypy bug that has been resolved but not merged
        # https://github.com/python/mypy/issues/9907
        if isinstance(self.identifier, str):  # type: ignore[unreachable]
            return False  # type: ignore[unreachable]
        return len(self.identifier) > 1

    def validate(self, row: Json) -> None:
        """Validate a record against the schema."""
        jsonschema.validate(row, self.data["schema"])

    def _resolve(self, ref: str) -> jsonschema.RefResolver:
        """Resolve the actual data type of a remote URI reference."""
        return jsonschema.RefResolver(ref, referrer=self)

    @property
    def has_parent_table(self) -> bool:
        return "parentTableID" in self

    @property
    def filters(self) -> Dict[str, Dict[str, str]]:
        """Fetch list of additional filters"""
        return dict(self["schema"].get("additionalFilters", {}))

    @property
    def relations(self) -> Dict[str, Dict[str, str]]:
        """Fetch list of additional (backwards or N-N) relations.

        This is a dictionary of names for existing forward relations
        in other tables with either the 'embedded' or 'summary'
        property
        """
        return dict(self["schema"].get("additionalRelations", {}))

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
        return cast(str, to_snake_case(model_name))  # mypy pleaser

    def db_name(self) -> str:
        """Returns the tablename for the database, prefixed with the schemaname.
        NB. `self.name` could have been changed by a 'shortname' in the schema.
        """

        from schematools.utils import shorten_name, to_snake_case

        if self.dataset is None:
            raise ValueError(
                "Cannot obtain a db_name from a DatasetTableSchema without a parent dataset."
            )
        table_name_parts = [self.dataset.id, self.name]
        if self.dataset.version is not None:
            is_default_table = (
                self.dataset.version.split(".")[0] == self.dataset.default_version.split(".")[0]
            )
            if not is_default_table:
                major, _minor, _patch = self.dataset.version.split(".")
                table_name_parts = [self.dataset.id, major, self.name]
        table_name = "_".join(table_name_parts)
        return shorten_name(to_snake_case(table_name), with_postfix=True)

    def get_fk_fields(self) -> Iterator[str]:
        """Generates fields names that contain a 1:N relation to a parent table"""
        fields_items = self["schema"]["properties"].items()
        field_schema = (
            DatasetFieldSchema(_parent_table=self, **{**spec, "id": _id})
            for _id, spec in fields_items
        )
        for field in field_schema:
            if field.relation:
                yield field.name


class DatasetFieldSchema(DatasetType):
    """A single field (column) in a table

    This class has an `id` property to uniquely
    address this datasetfield-schema in the scope of the `DatasetTableSchema`.
    This `id` is used in lots of places in the dynamic model generation in Django.

    There is also a `name` attribute, that is used for the autogeneration
    of tablenames that are used in postgreSQL.

    This `name` attribute is equal to the `id`, unless there is a `shortname`
    defined. In that case `name` is equal to the `shortname`.

    The `shortname` has been added for practical purposes, because there is a hard
    limitation on the length of column- and tablenames in databases like postgreSQL.

    """

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
        return self._id

    @property
    def name(self) -> str:
        """Table name, for display purposes only."""
        return cast(str, self.get("shortname", self._id))

    @property
    def has_shortname(self) -> bool:
        return self.get("shortname") is not None

    @property
    def description(self) -> Optional[str]:
        return self.get("description")

    @property
    def required(self) -> bool:
        return self._required

    @property
    def type(self) -> str:
        value = self.get("type")
        if not value:
            value = self.get("$ref")
            if not value:
                raise RuntimeError(f"No 'type' or '$ref' found in {self!r}")
        return str(value)

    @property
    def is_primary(self) -> bool:
        """When name is 'id' the field should be the primary key
        For compound keys (table.identifier has > 1 item), an 'id'
        field is autogenerated.
        """
        if self.table is None:
            return False
        return self.name == "id" or [self.name] == self.table.identifier

    @property
    def relation(self) -> Optional[str]:
        if self.type == "array":
            return None
        return self.get("relation")

    @property
    def nm_relation(self) -> Optional[str]:
        if self.type != "array":
            return None
        return self.get("relation")

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

    def get_dimension_fieldnames_for_relation(
        self, relation: Optional[str], nm_relation: Optional[str]
    ) -> Dict[str, Tuple[str, str]]:
        """Gets the dimension fieldnames."""
        if relation is None and nm_relation is None:
            return {}

        dataset_id, table_id = cast(str, relation or nm_relation).split(":")
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

        # Seems that mypy cannot infer the type from the assignment
        dimensions: Dict[str, Tuple[str, str]] = dataset_table.temporal.dimensions
        return dimensions if dimensions is not None else {}

    @property
    def sub_fields(self) -> Iterator[DatasetFieldSchema]:
        """Return the sub fields for a nested structure.

        For a nested object, fields are based on its properties,
        for an array of objects, fields are based on the properties
        of the "items" field.

        When subfields are added as part of an nm-relation
        those subfields will be prefixed with the name of the relation field.
        However, this is not the case for the so-called `dimension` fields
        of a temporal relation (e.g. `beginGeldigheid` and `eindGeldigheid`).
        """
        from schematools.utils import toCamelCase

        field_name_prefix = ""
        if self.is_object:
            # Field has direct sub fields (type=object)
            required = set(self.get("required", []))
            properties = self["properties"]
        elif self.is_array_of_objects and self.field_items is not None:
            # Field has an array of objects (type=array, items are objects)
            required = set(self.field_items.get("required") or ())
            properties = self.field_items["properties"]
        else:
            raise ValueError("Subfields are only possible for 'object' or 'array' fields.")

        relation = self.relation
        nm_relation = self.nm_relation
        if relation is not None or nm_relation is not None:
            field_name_prefix = self.name + RELATION_INDICATOR

        combined_dimension_fieldnames: Set[str] = set()
        for (_dimension, field_names) in self.get_dimension_fieldnames_for_relation(
            relation, nm_relation
        ).items():
            combined_dimension_fieldnames |= set(
                toCamelCase(fieldname) for fieldname in field_names
            )

        for id_, spec in properties.items():

            field_id = id_ if id_ in combined_dimension_fieldnames else f"{field_name_prefix}{id_}"
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

        XXX: What if source is not temporal, but target is temporal?
        Do we have a through table in that case?
        """

        return (self.is_array and self.nm_relation is not None) or (
            self.table is not None and self.table.is_temporal and self.relation is not None
        )

    @property
    def auth(self) -> FrozenSet[str]:
        """Auth of the field, if available, or None"""
        return _normalize_scopes(self.get("auth"))


class DatasetRow(DatasetType):
    """An actual instance of data"""

    def validate(self, schema: DatasetSchema) -> None:
        table = schema.get_table_by_id(self["table"])
        table.validate(self.data)


@total_ordering
class PermissionLevel(Enum):
    """The various levels that can be provided on specific fields."""

    # Higher values give higher preference. The numbers are arbitrary and for internal usage
    # allowing to test test "read > encoded" for example.
    read = 50
    encoded = 40
    random = 30
    letters = 10
    subobjects_only = 1  # allows to open a table only to access sub-fields
    none = 0  # means no permission.

    highest = read

    @classmethod
    def from_string(cls, value: Optional[str]) -> PermissionLevel:
        """Cast the string value to a permission level object."""
        if value is None:
            return cls.none
        elif "_" in value:
            # Anything with an underscore is internal
            raise ValueError("Invalid permission")
        else:
            return cls[value]

    def __str__(self):
        # Using the name as official value.
        return self.name

    def __bool__(self):
        """The 'none' level is recognized as "NO PERMISSION"."""
        # more direct then reading bool(self.value) as that goes through descriptors
        return self is not PermissionLevel.none

    def __lt__(self, other):
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

    def __post_init__(self):
        if self.level is PermissionLevel.none:
            # since profiles only grant permission,
            # having no permission is always from the schema.
            self.source = "schema"

    @classmethod
    def from_string(cls, value: Optional[str], source: Optional[str] = None) -> Permission:
        """Cast the string value to a permission level object."""
        if value is None:
            return cls(PermissionLevel.none, source=source)

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
        if self.level is PermissionLevel.read:
            return None
        elif self.level is PermissionLevel.letters:
            return lambda value: value[0 : int(self.sub_value)]
        else:
            raise NotImplementedError(f"Unsupported permission mode: {self.level}")


Permission.none = Permission(level=PermissionLevel.none)


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
        return cls(obj)

    @property
    def name(self) -> Optional[str]:
        """Name of Profile (if set)"""
        return self.get("name")

    @property
    def scopes(self) -> FrozenSet[str]:
        """One of these scopes should match in order to activate the profile."""
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
                return Permission(PermissionLevel.subobjects_only, source=f"{source}.fields.*")

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


@dataclass
class Temporal:
    """The temporal property of a Table.
    Describes validity of objects for tables where
    different versions of objects are valid over time.

    Attributes:
        identifier (str):
            The key to the property that uniquely identifies a specific
            version of an object from among other versions of the same object.

            This property combined with the fixed identifier forms a unique key for an object.

            These identifier properties are non-contiguous increasing integers.
            The latest version of an object will have the highest value for identifier.

        dimensions Dict[Tuple[str]]:
            Contains the attributes of objects that determine for what (time)period an object is valid.

            Dimensions is of type dict.
            A dimension is a tuple of the form "('valid_start', 'valid_end')",
            describing a closed set along the dimension for which an object is valid.

            Example:
                With dimensions = {"time":('valid_start', 'valid_end')}
                an_object will be valid on some_time if:
                some_time >= an_object.valid_start and some_time <= an_object.valid_end
    """

    identifier: str
    dimensions: Dict[str, Tuple[str, str]] = field(default_factory=dict)


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
