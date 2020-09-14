"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import json
import typing
from collections import UserDict
import jsonschema
from . import RELATION_INDICATOR


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
    def tables(self) -> typing.List[DatasetTableSchema]:
        """Access the tables within the file"""
        return [DatasetTableSchema(i, _parent_schema=self) for i in self["tables"]]

    def get_tables(
        self, include_nested=False, include_through=False,
    ) -> typing.List[DatasetTableSchema]:
        """List tables, including nested"""
        tables = self.tables
        if include_nested:
            tables += self.nested_tables
        if include_through:
            tables += self.through_tables
        return tables

    def get_table_by_id(self, table_id: str) -> DatasetTableSchema:
        for table in self.get_tables(include_nested=True):
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
                    "schema": {"$ref": f"/definitions/schema"},
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
            to_snake_case(part) for part in field.nm_relation.split(":")
        ]
        snakecased_fieldname = to_snake_case(field.name)
        sub_table_schema = dict(
            id=f"{left_table}_{snakecased_fieldname}",
            type="table",
            schema={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "required": ["schema"],
                "properties": {
                    "schema": {"$ref": f"/definitions/schema"},
                    left_table: {
                        "type": "integer",
                        "relation": f"{left_dataset}:{left_table}",
                    },
                    right_table: {
                        "type": "integer",
                        "relation": f"{right_dataset}:{right_table}",
                    },
                    **field["items"]["properties"],
                },
            },
        )
        return DatasetTableSchema(sub_table_schema, _parent_schema=self)

    @property
    def temporal(self):
        from schematools.utils import to_snake_case

        temporal_configuration = self.get("temporal", None)
        if temporal_configuration is None:
            return None

        for key, [start_field, end_field] in temporal_configuration.get(
            "dimensions", {}
        ).items():
            temporal_configuration["dimensions"][key] = [
                to_snake_case(start_field),
                to_snake_case(end_field),
            ]

        return temporal_configuration


class DatasetTableSchema(SchemaType):
    """The table within a dataset.
    This table definition follows the JSON Schema spec.
    """

    def __init__(self, *args, _parent_schema=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_schema = _parent_schema

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
            # Add extra field for relations of type object
            if field_schema.relation is not None and field_schema.is_object:
                for subfield_schema in field_schema.sub_fields:
                    yield subfield_schema
            yield field_schema

        # If compound key, add PK field
        # XXX we should check for an existing "id" field, avoid collisions
        if self.has_compound_key:
            yield DatasetFieldSchema(
                _name="id", _parent_table=self, _required=True, type="string"
            )

    @property
    def display_field(self):
        """Tell which fields can be used as display field."""
        return self["schema"].get("display", None)

    @property
    def is_temporal(self):
        """Indicates if this is a table with temporal charateristics """
        return self["schema"].get(
            "isTemporal", self._parent_schema.temporal is not None
        )

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


class DatasetFieldSchema(DatasetType):
    """ A single field (column) in a table """

    def __init__(
        self,
        *args,
        _name=None,
        _parent_table=None,
        _parent_field=None,
        _required=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._name = _name
        self._parent_table = _parent_table
        self._parent_field = _parent_field
        self._required = _required

    @property
    def table(self) -> typing.Optional[DatasetTableSchema]:
        """The table that this field is a part of"""
        return self._parent_table

    @property
    def parent_field(self) -> typing.Optional[DatasetFieldSchema]:
        """Provide access to the top-level field where is is a property for."""
        return self._parent_field

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> typing.Optional[str]:
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
        """ When name is 'id' the field should be the primary key
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
    def is_array(self) -> bool:
        """Tell whether the field references an array."""
        return self.get("type") == "array"

    @property
    def is_object(self) -> bool:
        """Tell whether the field references an object."""
        return self.get("type") == "object"

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
    def sub_fields(self) -> typing.List[DatasetFieldSchema]:
        """Return the fields for a nested object."""
        field_name_prefix = ""
        if self.is_object:
            # Field has direct sub fields (type=object)
            required = set(self.get("required", []))
            properties = self["properties"]
        elif self.is_nested_table:
            # Field has an array of objects (type=array)
            required = set(self.items.get("required") or ())
            properties = self.items["properties"]

        if self.relation is not None:
            field_name_prefix = self.name + RELATION_INDICATOR
            # field_name_prefix = self.relation.split(":")[1] + RELATION_INDICATOR
        required = set(self.get("required", []))
        for name, spec in properties.items():
            field_name = f"{field_name_prefix}{name}"
            yield DatasetFieldSchema(
                _name=field_name,
                _parent_table=self._parent_table,
                _parent_field=self,
                _required=(name in required),
                **spec,
            )

    @property
    def is_nested_table(self) -> bool:
        """
        Checks if field is a possible nested table.
        """
        return (
            self.get("type") == "array"
            and self.nm_relation is None
            and self.get("items", {}).get("type") == "object"
        )

    @property
    def is_through_table(self) -> bool:
        """
        Checks if field is a possible through table.
        """
        return (
            self.get("type") == "array"
            and self.nm_relation is not None
            and self.get("items", {}).get("type") == "object"
        )


class DatasetRow(DatasetType):
    """ An actual instance of data """

    def validate(self, schema: DatasetSchema):
        table = schema.get_table_by_id(self["table"])
        table.validate(self.data)


def is_possible_display_field(field: DatasetFieldSchema) -> bool:
    """See whether the field is a possible candidate as display field"""
    # TODO: the schema needs to provide a display field!
    return (
        field.type == "string"
        and "$ref" not in field
        and " " not in field.name
        and not field.name.endswith("_id")
    )


def get_db_table_name(table: DatasetTableSchema) -> str:
    """Generate the table name for a database schema."""
    from schematools.utils import to_snake_case

    dataset = table._parent_schema
    app_label = dataset.id
    table_id = table.id
    return to_snake_case(f"{app_label}_{table_id}")
