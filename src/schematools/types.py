"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import json
from collections import UserDict
from typing import Any, Callable, Dict, Iterator, List, NoReturn, Optional, TypeVar, Union

import jsonschema

from schematools import RELATION_INDICATOR
from schematools.datasetcollection import DatasetCollection

ST = TypeVar("ST", bound="SchemaType")


class SchemaType(UserDict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key: str) -> NoReturn:
        raise KeyError(f"No field named '{key}' exists in {self!r}")

    @property
    def id(self) -> str:
        return self["id"]

    @property
    def type(self) -> str:
        return self["type"]

    def json(self) -> str:
        return json.dumps(self.data)

    def json_data(self) -> Dict[str, Any]:
        return self.data

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> ST:
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

    def __init__(self, *args, use_dimension_fields: bool = False, **kwargs) -> None:
        """When initializing a datasets, a cache of related datasets
        can be added (at classlevel). Thus, we are able to get (temporal) info
        about the related datasets
        """
        super().__init__(*args, **kwargs)
        self.dataset_collection = DatasetCollection()
        self.dataset_collection.add_dataset(self)
        self._use_dimension_fields = use_dimension_fields

    @classmethod
    def from_file(cls, filename: str):
        """Open an Amsterdam schema from a file."""
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> DatasetSchema:
        """ Parses given dict and validates the given schema """
        if obj.get("type") != "dataset" or not isinstance(obj.get("tables"), list):
            raise ValueError("Invalid Amsterdam Schema file")

        return cls(obj)

    @property
    def title(self) -> str:
        """Title of the dataset (if set)"""
        return self.get("title")

    @property
    def description(self) -> str:
        """Description of the dataset (if set)"""
        return self.get("description")

    @property
    def url_prefix(self) -> str:
        """Dataset URL prefix"""
        return self.get("url_prefix", "")

    @property
    def identifier(self):
        """Which fields acts as identifier. (default is Django "pk" field)"""
        return self.get("identifier", "pk")

    @property
    def version(self):
        """Dataset Schema Version"""
        return self.get("version", None)

    @property
    def default_version(self):
        """Default version for this schema"""
        return self.get("default_version", self.version)

    @property
    def is_default_version(self):
        """Is this Default Dataset version.
        Defaults to True, in order to stay backwards compatible."""
        return self.default_version == self.version

    @property
    def auth(self):
        """Auth of the dataset (if set)"""
        return self.get("auth")

    def get_dataset_schema(self, dataset_id) -> DatasetSchema:
        return self.dataset_collection.get_dataset(dataset_id)

    @property
    def use_dimension_fields(self):
        """Indication if schema has to add extra dimension fields
        for relations
        """
        return self._use_dimension_fields

    @use_dimension_fields.setter
    def use_dimension_fields(self, value: bool) -> None:
        self._use_dimension_fields = value

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
        raise ValueError(
            f"Table '{table_id}' does not exist "
            f"in schema '{self.id}', available are: '{available}'"
        )

    @property
    def nested_tables(self) -> List[DatasetTableSchema]:
        """Access list of nested tables"""
        tables = []
        for table in self.tables:
            for field in table.fields:
                if field.is_nested_table:
                    tables.append(self.build_nested_table(table=table, field=field))
        return tables

    @property
    def through_tables(self) -> List[DatasetTableSchema]:
        """Access list of through_tables (for n-m relations) """
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
            "type": "table",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "parentTableID": table.id,
                "required": ["id", "schema"],
                "properties": {
                    "id": {"type": "integer/autoincrement", "description": ""},
                    "schema": {"$ref": "/definitions/schema"},
                    "parent": {"type": "integer", "relation": f"{self.id}:{table.id}"},
                    **field["items"]["properties"],
                },
            },
        }

        # When shortnames are in use for table or field
        # we need to add a shortname to the dynamically generated
        # schema definition.
        if field.has_shortname or table.has_shortname:
            snakecased_fieldname = to_snake_case(field.name)
            sub_table_schema["shortname"] = get_rel_table_identifier(
                len(self.id) + 1, table.name, snakecased_fieldname
            )
        return DatasetTableSchema(sub_table_schema, _parent_schema=self, nested_table=True)

    def build_through_table(
        self, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetTableSchema:
        from schematools.utils import get_rel_table_identifier, to_snake_case

        # Build the through_table for n-m relation
        # For relations, we have to use the real ids of the tables
        # and not the shortnames
        left_dataset = to_snake_case(self.id)
        left_table = to_snake_case(table.id)
        right_dataset, right_table = [
            to_snake_case(part) for part in field.nm_relation.split(":")[:2]
        ]

        snakecased_fieldname = to_snake_case(field.name)
        snakecased_field_id = to_snake_case(field.id)
        table_id = get_rel_table_identifier(len(self.id) + 1, table.id, snakecased_field_id)

        sub_table_schema = {
            "id": table_id,
            "type": "table",
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["schema"],
                "properties": {
                    "schema": {"$ref": "/definitions/schema"},
                    left_table: {
                        "type": "integer",
                        "relation": f"{left_dataset}:{left_table}",
                    },
                    snakecased_fieldname: {
                        "type": "integer",
                        "relation": f"{right_dataset}:{right_table}",
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

        # Only for object type relations we can add the fields of the
        # relation to the schema
        if field.is_array_of_objects:
            sub_table_schema["schema"]["properties"].update(field["items"]["properties"])

        return DatasetTableSchema(sub_table_schema, _parent_schema=self, through_table=True)

    def fetch_temporal(
        self, field_modifier: Optional[Callable] = None
    ) -> Optional[Dict[str, Union[str, Dict[str, List[str]]]]]:
        """The original implementation of 'temporal' already does
        a to_snake_case, however, we also need a version that
        leaves the fields in camelcase.
        """
        from schematools.utils import to_snake_case

        if field_modifier is None:
            field_modifier = to_snake_case

        temporal_configuration = self.get("temporal", None)
        if temporal_configuration is None:
            return None

        for key, [start_field, end_field] in temporal_configuration.get("dimensions", {}).items():
            temporal_configuration["dimensions"][key] = [
                field_modifier(start_field),
                field_modifier(end_field),
            ]

        return temporal_configuration

    @property
    def temporal(self):
        return self.fetch_temporal()


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
        self, *args, _parent_schema=None, nested_table=False, through_table=False, **kwargs
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
    def name(self) -> Optional[str]:
        return self.get("shortname", self.id)

    @property
    def has_shortname(self) -> bool:
        return self.get("shortname") is not None

    @property
    def dataset(self) -> Optional[DatasetSchema]:
        """The dataset that this table is part of."""
        return self._parent_schema

    @property
    def description(self) -> Optional[DatasetSchema]:
        """The description of the table as stated in the schema."""
        return self.get("description")

    @property
    def fields(self) -> Iterator[DatasetFieldSchema]:
        required = set(self["schema"]["required"])
        for id_, spec in self["schema"]["properties"].items():
            field_schema = DatasetFieldSchema(
                _id=id_, _parent_table=self, _required=(id_ in required), **spec
            )
            # Add extra fields for relations of type object
            # These fields are added to identify the different
            # components of a compound FK to a another table
            if field_schema.relation is not None and field_schema.is_object:
                for subfield_schema in field_schema.sub_fields:
                    yield subfield_schema
            yield field_schema

        # If compound key, add PK field
        # XXX we should check for an existing "id" field, avoid collisions
        if self.has_compound_key:
            yield DatasetFieldSchema(_id="id", _parent_table=self, _required=True, type="string")

    def get_fields_by_id(self, field_ids: List[str]) -> Iterator[DatasetFieldSchema]:
        for field in self.fields:
            if field.id in set(field_ids):
                yield field

    def get_field_by_id(self, field_id) -> Optional[DatasetFieldSchema]:
        for field_schema in self.fields:
            if field_schema.id == field_id:
                return field_schema

    def get_through_tables_by_id(self) -> List[DatasetTableSchema]:
        """Access list of through_tables (for n-m relations) for a single base table """
        tables = []
        for field in self.fields:
            if field.is_through_table:
                tables.append(self._parent_schema.build_through_table(table=self, field=field))
        return tables

    @property
    def display_field(self):
        """Tell which fields can be used as display field."""
        return self["schema"].get("display", None)

    def get_dataset_schema(self, dataset_id):
        """Return another datasets """
        return self._parent_schema.get_dataset_schema(dataset_id)

    @property
    def use_dimension_fields(self) -> bool:
        """Indication if schema has to add extra dimension fields
        for relations
        """
        return self._parent_schema.use_dimension_fields

    @property
    def temporal(self) -> Optional[Dict[str, Union[str, Dict[str, List[str]]]]]:
        """Return the temporal info from the dataset schema """
        return self._parent_schema.fetch_temporal(field_modifier=lambda x: x)

    @property
    def is_temporal(self) -> bool:
        """Indicates if this is a table with temporal charateristics """
        return self["schema"].get("isTemporal", self.temporal is not None)

    @property
    def main_geometry(self):
        """The main geometry field, if there is a geometry field available.
        Default to "geometry" for existing schemas without a mainGeometry field.
        """
        return self["schema"].get("mainGeometry", "geometry")

    @property
    def identifier(self) -> List[str]:
        """The main identifier field, if there is an identifier field available.
        Default to "id" for existing schemas without an identifier field.
        """
        identifier = self["schema"].get("identifier", ["id"])
        # Convert identifier to a list, to be backwards compatible with older schemas
        if not isinstance(identifier, list):
            identifier = [identifier]
        return identifier

    @property
    def has_compound_key(self) -> bool:
        if isinstance(self.identifier, str):
            return False
        return len(self.identifier) > 1

    def validate(self, row: Dict[str, Any]):
        """Validate a record against the schema."""
        jsonschema.validate(row, self.data["schema"])

    def _resolve(self, ref):
        """Resolve the actual data type of a remote URI reference."""
        return jsonschema.RefResolver(ref, referrer=self)

    @property
    def has_parent_table(self):
        return "parentTableID" in self["schema"]

    @property
    def filters(self):
        """Fetch list of additional filters"""
        return self["schema"].get("additionalFilters", {})

    @property
    def relations(self):
        """Fetch list of additional (backwards or N-N) relations

        This is a dictionary of names for existing forward relations
        in other tables with either the 'embedded' or 'summary'
        property
        """
        return self["schema"].get("additionalRelations", {})

    @property
    def auth(self):
        """Auth of the table (if set)"""
        return self.get("auth")

    @property
    def is_through_table(self) -> bool:
        """Indicates if table is an intersection table (n:m relation table) or base table"""
        return self.through_table

    @property
    def is_nested_table(self):
        """Indicates if table is an nested table"""
        return self.nested_table

    def model_name(self) -> str:
        """Returns model name for this table. Including version number, if needed."""

        from schematools.utils import to_snake_case

        model_name = self.id
        if self.dataset.version is not None and not self.dataset.is_default_version:
            model_name = f"{model_name}_{self.dataset.version}"
        return to_snake_case(model_name)

    def db_name(self) -> str:
        """Returns the tablename for the database, prefixed with the schemaname.
        NB. `self.name` could have been changed by a 'shortname' in the schema.
        """

        from schematools.utils import shorten_name, to_snake_case

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
            DatasetFieldSchema(_id=_id, _parent_table=self, **spec) for _id, spec in fields_items
        )
        for field in field_schema:
            if field.relation:
                yield field.name


class DatasetFieldSchema(DatasetType):
    """A single field (column) in a table

    This class has an `id` property (inherited from `SchemaType`) to uniquely
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
        *args,
        _id=None,
        _parent_table=None,
        _parent_field=None,
        _required=None,
        _temporal=False,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._id = _id
        self._parent_table = _parent_table
        self._parent_field = _parent_field
        self._required = _required
        self._temporal = _temporal

    @property
    def table(self) -> DatasetTableSchema:
        """The table that this field is a part of"""
        return self._parent_table

    @property
    def parent_field(self) -> Optional[DatasetFieldSchema]:
        """Provide access to the top-level field where is is a property for."""
        return self._parent_field

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def name(self) -> Optional[str]:
        """ If there is a 'shortname' in the field schema, it overrides the name """
        return self.get("shortname", self._id)

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
        return value

    @property
    def is_primary(self) -> bool:
        """When name is 'id' the field should be the primary key
        For compound keys (table.identifier has > 1 item), an 'id'
        field is autogenerated.
        """
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
    def is_temporal(self) -> bool:
        """Tell whether the field is added, because it has temporal charateristics """
        return self._temporal

    @property
    def is_geo(self) -> bool:
        """Tell whether the field references a geo object."""
        return "geojson.org" in self.get("$ref", "")

    @property
    def provenance(self) -> Optional[str]:
        """ Get the provenance info, if available, or None"""
        return self.get("provenance")

    @property
    def items(self) -> Dict[str, Any]:
        """Return the item definition for an array type."""
        return self.get("items", {}) if self.is_array else None

    @property
    def sub_fields(self) -> Iterator[DatasetFieldSchema]:
        """Returns the sub fields for a nested structure. For a
        nested object, fields are based on its properties,
        for an array of objects, fields are based on the properties of
        the "items" field.
        """
        field_name_prefix = ""
        if self.is_object:
            # Field has direct sub fields (type=object)
            required = set(self.get("required", []))
            properties = self["properties"]
        elif self.is_array_of_objects:
            # Field has an array of objects (type=array, items are objects)
            required = set(self.items.get("required") or ())
            properties = self.items["properties"]
        else:
            raise ValueError("Subfields are only possible for 'object' or 'array' fields.")

        relation = self.relation
        nm_relation = self.nm_relation
        if relation is not None or nm_relation is not None:
            field_name_prefix = self.name + RELATION_INDICATOR

        for id_, spec in properties.items():
            field_id = f"{field_name_prefix}{id_}"
            yield DatasetFieldSchema(
                _id=field_id,
                _parent_table=self._parent_table,
                _parent_field=self,
                _required=(id_ in required),
                **spec,
            )

        # Add temporal fields on the relation if the table is temporal
        # and the use of dimension fields is enabled for the schema
        if not self._parent_table.use_dimension_fields:
            return
        if relation is not None or nm_relation is not None:
            dataset_id, table_id = (relation or nm_relation).split(
                ":"
            )  # XXX what about loose rels?
            dataset_schema = self._parent_table.get_dataset_schema(dataset_id)
            if dataset_schema is None:
                return
            try:
                dataset_table = dataset_schema.get_table_by_id(
                    table_id, include_nested=False, include_through=False
                )
            except ValueError:
                # If we cannot get the table, we ignore the exception
                # and we do not generate temporal fields
                return
            if nm_relation is not None:
                field_name_prefix = ""
            if dataset_table.is_temporal:
                for dimension_fieldnames in dataset_table.temporal.get("dimensions", {}).values():
                    for dimension_fieldname in dimension_fieldnames:
                        field_name = f"{field_name_prefix}{dimension_fieldname}"
                        yield DatasetFieldSchema(
                            _id=field_name,
                            _parent_table=self._parent_table,
                            _parent_field=self,
                            _required=False,
                            _temporal=True,
                            **{"type": "string", "format": "date-time"},
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
        """
        return self.is_array and self.nm_relation is not None

    @property
    def auth(self) -> Optional[str]:
        """Auth of the field, if available, or None"""
        return self.get("auth")


class DatasetRow(DatasetType):
    """ An actual instance of data """

    def validate(self, schema: DatasetSchema):
        table = schema.get_table_by_id(self["table"])
        table.validate(self.data)


class ProfileSchema(SchemaType):
    """The complete profile object"""

    @classmethod
    def from_file(cls, filename: str):
        """Open an Amsterdam schema from a file."""
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]):
        """ Parses given dict and validates the given schema """
        return cls(obj)

    @property
    def name(self):
        """Name of Profile (if set)"""
        return self.get("name")

    @property
    def scopes(self):
        """Scopes of Profile (if set)"""
        return self.get("scopes")

    @property
    def datasets(self) -> Dict[str, ProfileDatasetSchema]:
        return {
            id: ProfileDatasetSchema(id, self, data)
            for id, data in self.get("datasets", {}).items()
        }


class ProfileDatasetSchema(DatasetType):
    """A schema inside the profile dataset"""

    def __init__(self, _id, _parent_schema=None, data: Optional[Dict[str, Any]] = None):
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

    @property
    def permissions(self) -> Optional[str]:
        """Global permissions for the dataset"""
        return self.get("permissions")

    @property
    def tables(self) -> Dict[str, ProfileTableSchema]:
        return {
            id: ProfileTableSchema(id, self, data) for id, data in self.get("tables", {}).items()
        }


class ProfileTableSchema(DatasetType):
    """A single table in the profile"""

    def __init__(self, _id, _parent_schema=None, data: Optional[Dict[str, Any]] = None):
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

    @property
    def permissions(self) -> Optional[str]:
        """Global permissions for the table"""
        return self.get("permissions")

    @property
    def fields(self) -> Dict[str, str]:
        """The fields with their permission keys"""
        return self.get("fields", {})

    @property
    def mandatory_filtersets(self) -> List[Dict[str, Any]]:
        """Tell whether the listing can only be requested with certain inputs.
        E.g. an API user may only list data when they supply the lastname + birthdate.

        Example value::

            [
              ["bsn", "lastname"],
              ["postcode", "regimes.aantal[gte]"]
            ]
        """
        return self.get("mandatoryFilterSets", [])
