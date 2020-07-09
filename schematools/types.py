"""Python types for the Amsterdam Schema JSON file contents."""
from __future__ import annotations

import json
import typing
from collections import UserDict
import jsonschema


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
    def identifier(self):
        return self.get("identifier", "pk")

    @property
    def tables(self) -> typing.List[DatasetTableSchema]:
        """Access the tables within the file"""
        return [DatasetTableSchema(i, _parent_schema=self) for i in self["tables"]]

    def get_tables(self, include_nested=False) -> typing.List[DatasetTableSchema]:
        """List tables, including nested"""
        tables = self.tables
        if include_nested:
            tables += self.nested_tables
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
            yield DatasetFieldSchema(
                _name=name, _parent_table=self, _required=(name in required), **spec
            )

    @property
    def display_field(self):
        """Tell which fields can be used as display field."""
        return self["schema"].get("display", None)

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
        return self["schema"].get("identifier", "id")

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
        return self.name == self.table.identifier

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
    def sub_fields(self) -> typing.List[DatasetFieldSchema]:
        """Return the fields for a nested object."""
        if not self.is_object:
            return

        required = set(self["required"])
        for name, spec in self["properties"].items():
            yield DatasetFieldSchema(
                _name=name,
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
