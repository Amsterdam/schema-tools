"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import json
import typing
from collections import UserDict
import jsonschema
from . import RELATION_INDICATOR
from schematools import MAX_TABLE_LENGTH, TMP_TABLE_POSTFIX


class SchemaType(UserDict):
    def __repr__(self):
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key):
        raise KeyError(f"No field named '{key}' exists in {self!r}")

    @property
    def id(self) -> str:
        return self["id"]

    @property
    def type(self) -> str:
        return self["type"]

    def json(self) -> str:
        return json.dumps(self.data)

    def json_data(self) -> dict:
        return self.data


class DatasetType(UserDict):
    def __repr__(self):
        return f"{self.__class__.__name__}({self.data!r})"

    def __missing__(self, key):
        raise KeyError(f"No field named '{key}' exists in {self!r}")


class DatasetSchema(SchemaType):
    """The schema of a dataset.
    This is a collection of JSON Schema's within a single file.
    """

    _datasets_cache = {}

    def __init__(
        self,
        *args,
        datasets_cache: typing.Dict[str, DatasetSchema] = None,
        use_dimension_fields: bool = False,
        **kwargs,
    ):
        """When initializing a datasets, a cache of related datasets
        can be added (at classlevel). Thus, we are able to get (temporal) info
        about the related datasets
        """
        super().__init__(*args, **kwargs)
        if datasets_cache is not None:
            self._datasets_cache = datasets_cache
        self._use_dimension_fields = use_dimension_fields

    def add_datasets_cache(self, datasets_cache: typing.Dict[str, DatasetSchema]):
        """ A bit hacky, we need some wrapping object for all datasets """
        self._datasets_cache = datasets_cache

    def add_dataset_to_cache(self, dataset: DatasetSchema):
        """ A bit hacky, we need some wrapping object for all datasets """
        self._datasets_cache[dataset.id] = dataset

    @classmethod
    def from_file(cls, filename: str):
        """Open an Amsterdam schema from a file."""
        with open(filename) as fh:
            return cls.from_dict(json.load(fh))

    @classmethod
    def from_dict(cls, obj: dict):
        """ Parses given dict and validates the given schema """
        if obj.get("type") != "dataset" or not isinstance(obj.get("tables"), list):
            raise ValueError("Invalid Amsterdam Schema file")

        return cls(obj)

    @property
    def title(self):
        """Title of the dataset (if set)"""
        return self.get("title")

    @property
    def description(self):
        """Description of the dataset (if set)"""
        return self.get("description")

    @property
    def identifier(self):
        """Which fields acts as identifier. (default is Django "pk" field)"""
        return self.get("identifier", "pk")

    @property
    def auth(self):
        """Auth of the dataset (if set)"""
        return self.get("auth")

    def get_dataset_schema(self, dataset_id):
        return self._datasets_cache.get(dataset_id)

    @property
    def use_dimension_fields(self):
        """Indication if schema has to add extra dimension fields
        for relations
        """
        return self._use_dimension_fields

    @use_dimension_fields.setter
    def use_dimension_fields(self, value: bool):
        self._use_dimension_fields = value

    @property
    def tables(self) -> typing.List[DatasetTableSchema]:
        """Access the tables within the file"""
        return [DatasetTableSchema(i, _parent_schema=self) for i in self["tables"]]

    def get_tables(
        self,
        include_nested=False,
        include_through=False,
    ) -> typing.List[DatasetTableSchema]:
        """List tables, including nested"""
        tables = self.tables
        if include_nested:
            tables += self.nested_tables
        if include_through:
            tables += self.through_tables
        return tables

    def get_table_by_id(
        self, table_id: str, include_nested=True, include_through=True
    ) -> DatasetTableSchema:
        for table in self.get_tables(
            include_nested=include_nested, include_through=include_through
        ):
            if table.id == table_id:
                return table

        available = "', '".join([table["id"] for table in self["tables"]])
        raise ValueError(
            f"Table '{table_id}' does not exist "
            f"in schema '{self.id}', available are: '{available}'"
        )

    @property
    def nested_tables(self) -> typing.List[DatasetTableSchema]:
        """Access list of nested tables"""
        tables = []
        for table in self.tables:
            for field in table.fields:
                if field.is_nested_table:
                    tables.append(self.build_nested_table(table=table, field=field))
        return tables

    @property
    def through_tables(self) -> typing.List[DatasetTableSchema]:
        """Access list of through_tables (for n-m relations) """
        tables = []
        for table in self.tables:
            for field in table.fields:
                if field.is_through_table:
                    tables.append(self.build_through_table(table=table, field=field))
        return tables

    def build_nested_table(self, table, field):
        # Map Arrays into tables.
        sub_table_schema = dict(
            id=f"{table.id}_{field.name}",
            originalID=field.name,
            type="table",
            schema={
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
        )
        return DatasetTableSchema(sub_table_schema, _parent_schema=self)

    def build_through_table(self, table, field):
        from schematools.utils import to_snake_case

        # Build the through_table for n-m relation
        left_dataset = to_snake_case(self.id)
        left_table = to_snake_case(table.id)
        right_dataset, right_table = [
            to_snake_case(part) for part in field.nm_relation.split(":")[:2]
        ]
        snakecased_fieldname = to_snake_case(field.name)
        table_id = get_db_table_name(table, snakecased_fieldname)
        # dso-api expects the dataset_id (as prefix) not part of the table_id
        table_id = "_".join(table_id.split("_")[1:])
        sub_table_schema = dict(
            id=table_id,
            type="table",
            schema={
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
                    **field["items"]["properties"],
                },
            },
        )
        return DatasetTableSchema(sub_table_schema, _parent_schema=self, through_table=True)

    def fetch_temporal(self, field_modifier=None):
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


class DatasetTableSchema(DatasetSchema):
    """The table within a dataset.
    This table definition follows the JSON Schema spec.
    """

    def __init__(self, *args, _parent_schema=None, through_table=False, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_schema = _parent_schema
        self.through_table = through_table

        if self.get("type") != "table":
            raise ValueError("Invalid Amsterdam schema table data")

        if not self["schema"].get("$schema", "").startswith("http://json-schema.org/"):
            raise ValueError("Invalid JSON-schema contents of table")

    @property
    def dataset(self) -> typing.Optional[DatasetSchema]:
        """The dataset that this table is part of."""
        return self._parent_schema

    @property
    def fields(self):
        required = set(self["schema"]["required"])
        for name, spec in self["schema"]["properties"].items():
            field_schema = DatasetFieldSchema(
                _name=name, _parent_table=self, _required=(name in required), **spec
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
            yield DatasetFieldSchema(_name="id", _parent_table=self, _required=True, type="string")

    def get_fields_by_id(self, field_names) -> typing.Generator[DatasetFieldSchema, None, None]:
        for field in self.fields:
            if field.name in set(field_names):
                yield field

    def get_field_by_id(self, field_name) -> typing.Optional[DatasetFieldSchema]:
        for field_schema in self.fields:
            if field_schema.name == field_name:
                return field_schema

    def get_through_tables_by_id(self) -> typing.List[DatasetTableSchema]:
        """Access list of through_tables (for n-m relations) for a single base table """
        tables = []
        for field in self.fields:
            if field.is_through_table:
                tables.append(super().build_through_table(table=self, field=field))
        return tables

    @property
    def display_field(self):
        """Tell which fields can be used as display field."""
        return self["schema"].get("display", None)

    def get_dataset_schema(self, dataset_id):
        """Return another datasets """
        return self._parent_schema.get_dataset_schema(dataset_id)

    @property
    def use_dimension_fields(self):
        """Indication if schema has to add extra dimension fields
        for relations
        """
        return self._parent_schema.use_dimension_fields

    @property
    def temporal(self):
        """Return the temporal info from the dataset schema """
        return self._parent_schema.fetch_temporal(field_modifier=lambda x: x)

    @property
    def is_temporal(self):
        """Indicates if this is a table with temporal charateristics """
        return self["schema"].get("isTemporal", self.temporal is not None)

    @property
    def main_geometry(self):
        """The main geometry field, if there is a geometry field available.
        Default to "geometry" for existing schemas without a mainGeometry field.
        """
        return self["schema"].get("mainGeometry", "geometry")

    @property
    def identifier(self):
        """The main identifier field, if there is an identifier field available.
        Default to "id" for existing schemas without an identifier field.
        """
        identifier = self["schema"].get("identifier", ["id"])
        # Convert identifier to a list, to be backwards compatible with older schemas
        if not isinstance(identifier, list):
            identifier = [identifier]
        return identifier

    @property
    def has_compound_key(self):
        if isinstance(self.identifier, str):
            return False
        return len(self.identifier) > 1

    def validate(self, row: dict):
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
    def is_through_table(self):
        """Indicates if table is an intersection table (n:m relation table) or base table"""
        return self.through_table

    def db_name(self, through_table_field_name=None, db_table_name=None):
        """Returns the database implementation name of a table.
        TODO: Get database name from the JSON schema specification by defining 'shortname'.
        For now using existing function that is already used in the GOB data (NDJson files)
        """
        # for n:m tables, use the relating field_name as part of DB table name
        # i.e. heeft_verblijfsobjecten
        return get_db_table_name(
            self,
            through_table_field_name=through_table_field_name,
            db_table_name=db_table_name,
        )


class DatasetFieldSchema(DatasetType):
    """ A single field (column) in a table """

    def __init__(
        self,
        *args,
        _name=None,
        _parent_table=None,
        _parent_field=None,
        _required=None,
        _temporal=False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._name = _name
        self._parent_table = _parent_table
        self._parent_field = _parent_field
        self._required = _required
        self._temporal = _temporal

    @property
    def table(self) -> typing.Optional[DatasetTableSchema]:
        """The table that this field is a part of"""
        return self._parent_table

    @property
    def parent_field(self) -> typing.Optional[DatasetFieldSchema]:
        """Provide access to the top-level field where is is a property for."""
        return self._parent_field

    @property
    def name(self) -> typing.Optional[str]:
        return self._name

    @property
    def description(self) -> typing.Optional[str]:
        return self.get("description")

    @property
    def required(self) -> typing.Optional[bool]:
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
    def relation(self) -> typing.Optional[str]:
        if self.type == "array":
            return None
        return self.get("relation")

    @property
    def nm_relation(self) -> typing.Optional[str]:
        if self.type != "array":
            return None
        return self.get("relation")

    @property
    def format(self) -> typing.Optional[str]:
        return self.get("format")

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
    def provenance(self) -> typing.Optional[str]:
        """ Get the provenance info, if available, or None"""
        return self.get("provenance")

    @property
    def items(self) -> typing.Optional[dict]:
        """Return the item definition for an array type."""
        return self.get("items", {}) if self.is_array else None

    @property
    def sub_fields(self) -> typing.Generator[DatasetFieldSchema, None, None]:
        """Returns the sub fields for a nested structure. For a
        nested object, fields are based on its properties,
        for an array, fields are based on the properties of
        the "items" field.
        """
        field_name_prefix = ""
        if self.is_object:
            # Field has direct sub fields (type=object)
            required = set(self.get("required", []))
            properties = self["properties"]
        elif self.is_array:
            # Field has an array of objects (type=array)
            required = set(self.items.get("required") or ())
            properties = self.items["properties"]
        else:
            raise ValueError("Subfields are only possible for 'object' or 'array' fields.")

        relation = self.relation
        nm_relation = self.nm_relation
        if relation is not None or nm_relation is not None:
            field_name_prefix = self.name + RELATION_INDICATOR

        for name, spec in properties.items():
            field_name = f"{field_name_prefix}{name}"
            yield DatasetFieldSchema(
                _name=field_name,
                _parent_table=self._parent_table,
                _parent_field=self,
                _required=(name in required),
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
                            _name=field_name,
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
        return self.get("type") == "array" and self.get("items", {}).get("type") == "object"

    @property
    def is_nested_table(self) -> bool:
        """
        Checks if field is a possible nested table.
        """
        return self.is_array and self.nm_relation is None

    @property
    def is_through_table(self) -> bool:
        """
        Checks if field is a possible through table.
        """
        return self.is_array and self.nm_relation is not None

    @property
    def auth(self) -> typing.Optional[str]:
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
    def from_dict(cls, obj: dict):
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
    def datasets(self) -> typing.Dict[str, ProfileDatasetSchema]:
        return {
            id: ProfileDatasetSchema(id, self, data)
            for id, data in self.get("datasets", {}).items()
        }


class ProfileDatasetSchema(DatasetType):
    """A schema inside the profile dataset"""

    def __init__(self, _id, _parent_schema=None, data: typing.Optional[dict] = None):
        super().__init__(data)
        self._id = _id
        self._parent_schema = _parent_schema

    @property
    def id(self) -> str:
        return self._id

    @property
    def profile(self) -> typing.Optional[ProfileSchema]:
        """The profile that this definition is part of."""
        return self._parent_schema

    @property
    def permissions(self) -> typing.Optional[str]:
        """Global permissions for the dataset"""
        return self.get("permissions")

    @property
    def tables(self) -> typing.Dict[str, ProfileTableSchema]:
        return {
            id: ProfileTableSchema(id, self, data) for id, data in self.get("tables", {}).items()
        }


class ProfileTableSchema(DatasetType):
    """A single table in the profile"""

    def __init__(self, _id, _parent_schema=None, data: typing.Optional[dict] = None):
        super().__init__(data)
        self._id = _id
        self._parent_schema = _parent_schema

    @property
    def id(self) -> str:
        return self._id

    @property
    def dataset(self) -> typing.Optional[ProfileDatasetSchema]:
        """The profile that this definition is part of."""
        return self._parent_schema

    @property
    def permissions(self) -> typing.Optional[str]:
        """Global permissions for the table"""
        return self.get("permissions")

    @property
    def fields(self) -> typing.Dict[str, str]:
        """The fields with their permission keys"""
        return self.get("fields", {})

    @property
    def mandatory_filtersets(self) -> typing.List[dict]:
        """Tell whether the listing can only be requested with certain inputs.
        E.g. an API user may only list data when they supply the lastname + birthdate.

        Example value::

            [
              ["bsn", "lastname"],
              ["postcode", "regimes.aantal[gte]"]
            ]
        """
        return self.get("mandatoryFilterSets", [])


def get_db_table_name(
    table: DatasetTableSchema, through_table_field_name=None, db_table_name=None
) -> str:
    """Generate the table name for a database schema."""
    # import within function to avoid a circular import with utils.py
    from schematools.utils import to_snake_case

    dataset = table._parent_schema
    app_label = dataset.id
    table_id = table.id
    if db_table_name is None:
        db_table_name = f"{app_label}_{table_id}"
    through_table_field_name = "_" + through_table_field_name if through_table_field_name else ""
    return to_snake_case(f"{db_table_name}{through_table_field_name}").replace("-", "_")[
        : MAX_TABLE_LENGTH - len(TMP_TABLE_POSTFIX)
    ]
