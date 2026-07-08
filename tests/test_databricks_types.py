from __future__ import annotations

import json
from types import SimpleNamespace
from typing import cast

import pytest
from databricks.sdk.service.catalog import EntityTagAssignment, TableInfo

from schematools.contrib.databricks.types import (
    DatabricksInfo,
    Tag,
    Tags,
    as_datetime,
    as_integer,
    as_number,
    as_semver,
    as_type,
    crs,
    dataclass_attr,
    format,
    geo,
    list_or_string,
    status,
)


def test_tags_from_tag_assignments_filters_schema_tags() -> None:
    tag_assignments = [
        SimpleNamespace(tag_key="schema:title", tag_value="Title", entity_type="table"),
        SimpleNamespace(tag_key="other:title", tag_value="Ignored", entity_type="table"),
        SimpleNamespace(tag_key="schema:type", tag_value="string", entity_type="column"),
    ]

    tags = Tags.from_tag_assignments(cast(list[EntityTagAssignment], tag_assignments))

    assert list(tags) == [
        Tag(key="title", value="Title", type="tables"),
        Tag(key="type", value="string", type="columns"),
    ]


def test_databricks_info_validates_and_renders_table_json() -> None:
    table_tags = Tags(
        _tags=[
            Tag(key="id", value="buildings", type="tables"),
            Tag(key="version", value="1.2.3", type="tables"),
            Tag(key="identifier", value="id;code", type="tables"),
            Tag(key="required", value="schema;name", type="tables"),
            Tag(key="unknown", value="boom", type="tables"),
        ]
    )
    column_tags = {
        "geometry": Tags(
            _tags=[
                Tag(key="$ref", value="Point", type="columns"),
                Tag(key="type", value="string", type="columns"),
                Tag(key="format", value="date", type="columns"),
            ]
        ),
        "name": Tags(_tags=[Tag(key="minLength", value="2", type="columns")]),
    }
    table_info = cast(
        TableInfo,
        SimpleNamespace(columns=[SimpleNamespace(name="geometry"), SimpleNamespace(name="name")]),
    )

    info = DatabricksInfo(
        catalog="main",
        schema="default",
        table_name="buildings_table",
        table_info=table_info,
        table_tags=table_tags,
        column_tags=column_tags,
    )

    assert info.table_id == "buildings"
    assert info.errors == [
        "Unknown table or schema tag: schema:unknown",
        "Mutually exclusive tags for geometry: schema:type and schema:$ref",
    ]

    payload = json.loads(info.json)

    assert payload["id"] == "buildings"
    assert payload["version"] == "1.2.3"
    assert payload["schema"]["identifier"] == ["id", "code"]
    assert payload["schema"]["required"] == ["schema", "name"]
    assert payload["schema"]["properties"]["geometry"]["type"] == "string"
    assert payload["schema"]["properties"]["geometry"]["format"] == "date"
    assert (
        payload["schema"]["properties"]["geometry"]["$ref"]
        == "https://geojson.org/schema/Point.json"
    )
    assert payload["schema"]["properties"]["name"]["minLength"] == 2


def test_databricks_info_reports_explicit_none_values() -> None:
    table_info = cast(TableInfo, SimpleNamespace(columns=[]))
    table_tags = Tags(_tags=[Tag(key="auth", value=None, type="tables")])

    info = DatabricksInfo(
        catalog="main",
        schema="default",
        table_name="buildings_table",
        table_info=table_info,
        table_tags=table_tags,
        column_tags={},
    )

    assert info.errors == ["auth: Value cannot be None"]


@pytest.mark.parametrize(
    ("spec", "attr", "value", "expected_errors"),
    [
        (list_or_string, "auth", None, ["auth: Value cannot be None"]),
        (
            as_number,
            "maximum",
            "not-a-number",
            ["maximum: Value 'not-a-number' is not a valid number"],
        ),
        (
            as_integer,
            "minLength",
            "not-a-number",
            ["minLength: Value 'not-a-number' is not a valid number"],
        ),
        (
            as_semver,
            "version",
            "1.2",
            ["version: Value '1.2' is not a valid semantic version (x.y.z)"],
        ),
        (
            as_datetime,
            "dateCreated",
            "yesterday",
            ["dateCreated: Value 'yesterday' is not a valid ISO 8601 datetime"],
        ),
        (
            status,
            "status",
            "beta",
            ["status: Value 'beta' is not in the allowed values: ('stable', 'under_development')"],
        ),
        (
            as_type,
            "type",
            "date",
            [
                "type: Value 'date' is not in the allowed values: "
                "('string', 'number', 'integer', 'boolean', 'array', 'object', 'null')"
            ],
        ),
        (
            format,
            "format",
            "uuid",
            [
                "format: Value 'uuid' is not in the allowed values: "
                "('date-time', 'date', 'time', 'duration', 'email', 'idn-email', 'uri', "
                "'uri-reference', 'hostname', 'idn-hostname', 'ipv4', 'ipv6', 'iri', "
                "'iri-reference', 'json')"
            ],
        ),
        (
            geo,
            "$ref",
            "Circle",
            [
                "$ref: Value 'Circle' is not in the allowed values: "
                "('Point', 'LineString', 'Polygon', 'MultiPolygon', 'Geometry', "
                "'MultiLineString', 'MultiPoint')"
            ],
        ),
        (
            crs,
            "crs",
            "EPSG:9999",
            [
                "crs: Value 'EPSG:9999' is not in the allowed values: ('EPSG:4326', 'EPSG:28992', 'EPSG:7415')"
            ],
        ),
        (
            dataclass_attr,
            "dataclass",
            "stream",
            [
                "dataclass: Value 'stream' is not in the allowed values: "
                "('structured', 'blob', 'event')"
            ],
        ),
    ],
)
def test_attribute_spec_errors(spec, attr: str, value, expected_errors: list[str]) -> None:
    assert spec.errors(attr, value) == expected_errors
