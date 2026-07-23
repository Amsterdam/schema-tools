import json
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import Literal

from databricks.sdk.service.catalog import EntityTagAssignment

from schematools.naming import toCamelCase


@dataclass
class AttributeSpec:
    validators: list[Callable]
    transformers: list[Callable]

    def errors(self, attr, value) -> list[str]:
        errors = []
        for validator in self.validators:
            try:
                validator(value)
            except ValueError as e:
                errors.append(f"{attr}: {e}")
                break  # Stop further validation if one fails
        return errors

    def transform(self, value):
        for transformer in self.transformers:
            value = transformer(value)
        return value


def enum(*allowed_values):
    def validate(value):
        if value not in allowed_values:
            raise ValueError(f"Value '{value}' is not in the allowed values: {allowed_values}")
        return value

    return validate


def not_none(value):
    if value is None:
        raise ValueError("Value cannot be None")
    return value


def maybe_list(val: str):
    if ";" in val:
        return [v.strip() for v in val.split(";")]
    return val


def valid_number(val: str):
    try:
        float(val)
    except ValueError:
        raise ValueError(f"Value '{val}' is not a valid number") from None


def valid_datetime(val: str):
    try:
        datetime.fromisoformat(val)
    except ValueError:
        raise ValueError(f"Value '{val}' is not a valid ISO 8601 datetime") from None


def semver(val: str):
    parts = val.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"Value '{val}' is not a valid semantic version (x.y.z)")
    return val


BASE_TABLE_SCHEMA: dict = {
    "type": "table",
    "version": "1.0.0",
    "status": "stable",
    "schema": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": False,
        "identifier": "id",
        "required": ["schema"],
        "display": "id",
        "properties": {
            "schema": {
                "$ref": "https://schemas.data.amsterdam.nl/schema@v4.2.0#/definitions/schema"
            }
        },
    },
}

as_is = AttributeSpec(validators=[], transformers=[])
as_list = AttributeSpec(validators=[not_none], transformers=[lambda v: v.split(";")])
as_number = AttributeSpec(validators=[not_none, valid_number], transformers=[float])
as_integer = AttributeSpec(validators=[not_none, valid_number], transformers=[int])
as_semver = AttributeSpec(validators=[not_none, semver], transformers=[])
as_datetime = AttributeSpec(validators=[not_none, valid_datetime], transformers=[])
status = AttributeSpec(validators=[not_none, enum("stable", "under_development")], transformers=[])
geo = AttributeSpec(
    validators=[
        not_none,
        enum(
            "Point",
            "LineString",
            "Polygon",
            "MultiPolygon",
            "Geometry",
            "MultiLineString",
            "MultiPoint",
        ),
    ],
    transformers=[lambda v: f"https://geojson.org/schema/{v}.json"],
)
crs = AttributeSpec(
    validators=[not_none, enum("EPSG:4326", "EPSG:28992", "EPSG:7415")],
    transformers=[],
)
list_or_string = AttributeSpec(validators=[not_none], transformers=[maybe_list])
dataclass_attr = AttributeSpec(
    validators=[not_none, enum("structured", "blob", "event")],
    transformers=[],
)
as_type = AttributeSpec(
    validators=[
        not_none,
        enum("string", "number", "integer", "boolean", "array", "object", "null"),
    ],
    transformers=[],
)
format = AttributeSpec(
    validators=[
        not_none,
        enum(
            "date-time",
            "date",
            "time",
            "duration",
            "email",
            "idn-email",
            "uri",
            "uri-reference",
            "hostname",
            "idn-hostname",
            "ipv4",
            "ipv6",
            "iri",
            "iri-reference",
            "json",
        ),
    ],
    transformers=[],
)

TABLE_ATTRIBUTES = {
    "id": as_is,
    "version": as_semver,
    "status": status,
    "title": as_is,
    "description": as_is,
    "shortname": as_is,
    "derivedFrom": as_list,
    "auth": list_or_string,
    "rowLevelAuth": as_is,
    "reasonsNonPublic": as_list,
    "provenance": as_is,
    "dateCreated": as_datetime,
    "dateModified": as_datetime,
    "license": as_is,
    "temporal": as_is,
    "subresources": as_is,
    "crs": crs,
    "dataclass": dataclass_attr,
    "zoom": as_is,
}

TABLE_SCHEMA_ATTRIBUTES = {
    "required": as_list,
    "display": as_is,
    "mainGeometry": as_is,
    "identifier": list_or_string,
}

COLUMN_ATTRIBUTES = {
    "type": as_type,
    "$ref": geo,
    "title": as_is,
    "description": as_is,
    "auth": list_or_string,
    "filterAuth": list_or_string,
    "reasonsNonPublic": as_list,
    "provenance": as_is,
    "shortname": as_is,
    "unit": as_is,
    "relation": as_is,
    "crs": crs,
    "maximum": as_number,
    "minimum": as_number,
    "items": as_is,
    "exclusiveMaximum": as_integer,
    "multipleOf": as_number,
    "minLength": as_integer,
    "maxLength": as_integer,
    "contentEncoding": as_is,
    "enum": as_list,
    "format": format,
}

MUTUALLY_EXCLUSIVE_ATTRIBUTES = [
    ("type", "$ref"),
    ("maximum", "exclusiveMaximum"),
]

SCHEMA_TYPES = {
    "string": "string",
    "int": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "timestamp": "datetime",
    "date": "date",
    "boolean": "boolean",
    "double": "number",
    "float": "number",
}


@dataclass
class Tag:
    key: str
    value: str | None
    type: Literal["columns", "tables"] = "tables"


@dataclass
class Tags:
    _tags: list[Tag]

    @classmethod
    def from_tag_assignments(cls, tag_assignments: Iterable[EntityTagAssignment]) -> "Tags":
        tags = [
            Tag(
                key=tag.tag_key[len("schema:") :],
                value=tag.tag_value,
                type="columns" if tag.entity_type == "column" else "tables",
            )
            for tag in tag_assignments
            if tag.tag_key.startswith("schema:")
        ]
        return cls(_tags=tags)

    @classmethod
    def from_tag_list(
        cls, tag_list: list[dict[str, str]], tag_type: Literal["columns", "tables"]
    ) -> "Tags":
        tags = [
            Tag(
                key=tag["name"][len("schema:") :],
                value=tag.get("value"),
                type=tag_type,
            )
            for tag in tag_list
            if tag["name"].startswith("schema:")
        ]
        return cls(_tags=tags)

    def __iter__(self):
        return iter(self._tags)

    def __contains__(self, key: str) -> bool:
        return any(tag.key == key for tag in self._tags)

    def __getitem__(self, key: str) -> str | None:
        return next((tag.value for tag in self._tags if tag.key == key), None)


@dataclass
class DatabricksInfo:
    catalog: str
    schema: str
    table_name: str
    table_data: tuple[str | None, list[dict[str, str]]] | None
    column_data: list[tuple[str, str, str | None, str | None, str, list[dict[str, str]]]] | None
    table_tags: Tags
    column_tags: dict[str, Tags]
    errors: list[str] = field(default_factory=list)

    def _collect_spec_errors(self, tags: Tags, specs: dict[str, AttributeSpec]) -> list[str]:
        errors = []
        for attr, spec in specs.items():
            if attr not in tags:
                continue
            value = tags[attr]
            errors.extend(spec.errors(attr, value))
        return errors

    def _apply_tag_specs(self, target: dict, tags: Tags, specs: dict[str, AttributeSpec]) -> None:
        for attr, spec in specs.items():
            if attr not in tags:
                continue
            value = tags[attr]
            if value is None and not_none in spec.validators:
                # We only add None values if the validator allows it.
                continue
            if attr == "$ref":
                target.pop("type", None)  # Remove 'type' if '$ref' is present
            target[attr] = spec.transform(value)

    def _validate_table_tags(self) -> list[str]:
        errors = []
        allowed_keys = set(TABLE_ATTRIBUTES) | set(TABLE_SCHEMA_ATTRIBUTES)
        for tag in self.table_tags:
            if tag.key not in allowed_keys:
                errors.append(f"Unknown table or schema tag: schema:{tag.key}")
        errors.extend(self._collect_spec_errors(self.table_tags, TABLE_SCHEMA_ATTRIBUTES))
        errors.extend(self._collect_spec_errors(self.table_tags, TABLE_ATTRIBUTES))
        return errors

    def _validate_column_tags(self) -> list[str]:
        errors = []
        for column_name, tags in self.column_tags.items():
            for tag in tags:
                if tag.key not in COLUMN_ATTRIBUTES:
                    errors.append(f"Unknown column tag for {column_name}: schema:{tag.key}")

            tag_keys = {tag.key for tag in tags}
            for attr1, attr2 in MUTUALLY_EXCLUSIVE_ATTRIBUTES:
                if attr1 in tag_keys and attr2 in tag_keys:
                    errors.append(
                        f"Mutually exclusive tags for {column_name}: schema:{attr1} and "
                        f"schema:{attr2}"
                    )

            errors.extend(self._collect_spec_errors(tags, COLUMN_ATTRIBUTES))
        return errors

    def _build_column_schema(self, column_name: str, type: str, comment: str) -> dict:
        column_schema = {
            "title": column_name,
            "type": SCHEMA_TYPES.get(type),
            "description": comment,
        }
        tags = self.column_tags.get(column_name)
        if tags is not None:
            self._apply_tag_specs(column_schema, tags, COLUMN_ATTRIBUTES)
        return column_schema

    def __post_init__(self):
        """
        Validate the tags and collect errors.
        """
        self.errors.extend(self._validate_table_tags())
        self.errors.extend(self._validate_column_tags())

    @property
    def table_id(self) -> str:
        return toCamelCase(self.table_tags["id"] or self.table_name)

    def get_base_schema(self) -> dict:
        schema = {"id": self.table_id}
        schema.update(deepcopy(BASE_TABLE_SCHEMA))
        return schema

    @cached_property
    def dict(self) -> dict:
        schema = self.get_base_schema()
        self._apply_tag_specs(schema["schema"], self.table_tags, TABLE_SCHEMA_ATTRIBUTES)
        self._apply_tag_specs(schema, self.table_tags, TABLE_ATTRIBUTES)
        for name, type, _nullable, _default, comment, _tags in self.column_data or []:
            schema["schema"]["properties"][toCamelCase(name)] = self._build_column_schema(
                name, type, comment
            )
        return schema

    @cached_property
    def json(self) -> str:
        return json.dumps(self.dict, indent=2)
