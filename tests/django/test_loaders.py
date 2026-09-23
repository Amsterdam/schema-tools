from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from schematools.contrib.django.loaders import DatabaseSchemaLoader
from schematools.exceptions import DatasetNotFound


class QuerySetStub:
    def __init__(self, rows=None, scalar_rows=None):
        self.rows = rows or []
        self.scalar_rows = scalar_rows or {}
        self.filter_calls = []

    def filter(self, **kwargs):
        self.filter_calls.append(kwargs)
        return self

    def values_list(self, *fields, flat=False):
        if flat:
            return self.scalar_rows[fields[0]]
        return self.rows


def test_get_all_datasets_filters_and_converts_rows(monkeypatch):
    loader = DatabaseSchemaLoader()
    queryset = QuerySetStub(
        rows=[
            (
                "datasets/example",
                '{"id": "example", "type": "dataset"}',
                "select 1",
            )
        ]
    )

    monkeypatch.setattr(
        "schematools.contrib.django.loaders.Dataset.objects",
        SimpleNamespace(all=lambda: queryset),
    )
    monkeypatch.setattr(
        loader,
        "_as_dataset",
        lambda schema_data, view_sql: {"schema_data": schema_data, "view_sql": view_sql},
    )

    datasets = loader._get_all_datasets(enable_db=True)

    assert queryset.filter_calls == [{"enable_db": True}]
    assert datasets == {
        "datasets/example": {
            "schema_data": '{"id": "example", "type": "dataset"}',
            "view_sql": "select 1",
        }
    }


def test_get_all_datasets_without_enable_db_skips_filter(monkeypatch):
    loader = DatabaseSchemaLoader()
    queryset = QuerySetStub(
        rows=[("datasets/example", '{"id": "example", "type": "dataset"}', None)]
    )

    monkeypatch.setattr(
        "schematools.contrib.django.loaders.Dataset.objects",
        SimpleNamespace(all=lambda: queryset),
    )
    monkeypatch.setattr(loader, "_as_dataset", lambda schema_data, view_sql: schema_data)

    datasets = loader._get_all_datasets()

    assert queryset.filter_calls == []
    assert datasets == {"datasets/example": '{"id": "example", "type": "dataset"}'}


def test_get_dataset_returns_converted_dataset(monkeypatch):
    loader = DatabaseSchemaLoader()
    queryset = QuerySetStub(
        scalar_rows={
            "schema_data": ['{"id": "example", "type": "dataset"}'],
            "view_data": ["select 1"],
        }
    )
    filter_calls = []

    monkeypatch.setattr(
        "schematools.contrib.django.loaders.Dataset.objects",
        SimpleNamespace(
            filter=lambda **kwargs: filter_calls.append(kwargs) or queryset,
        ),
    )
    monkeypatch.setattr(
        loader, "_as_dataset", lambda schema_data, view_sql: (schema_data, view_sql)
    )

    dataset = loader._get_dataset("ExampleDataset")

    assert filter_calls == [{"name": "example_dataset"}]
    assert dataset == ('{"id": "example", "type": "dataset"}', "select 1")


def test_get_dataset_raises_dataset_not_found(monkeypatch):
    loader = DatabaseSchemaLoader()
    queryset = QuerySetStub(
        scalar_rows={
            "schema_data": [],
            "view_data": [],
        }
    )

    monkeypatch.setattr(
        "schematools.contrib.django.loaders.Dataset.objects",
        SimpleNamespace(filter=lambda **_kwargs: queryset),
    )

    with pytest.raises(DatasetNotFound, match="Dataset `missing` not found."):
        loader._get_dataset("missing")


def test_get_table_is_not_supported():
    loader = DatabaseSchemaLoader()

    with pytest.raises(NotImplementedError, match="don't support versioned tables"):
        loader._get_table(SimpleNamespace(), "v1")


def test_as_dataset_builds_dataset_schema(monkeypatch):
    loader = DatabaseSchemaLoader()
    dataset_calls = []

    def fake_dataset_schema(schema_data, view_sql, loader):
        dataset_calls.append((schema_data, view_sql, loader))
        return "dataset-schema"

    monkeypatch.setattr("schematools.contrib.django.loaders.DatasetSchema", fake_dataset_schema)

    result = loader._as_dataset('{"id": "example"}', "select 1")

    assert result == "dataset-schema"
    assert dataset_calls == [({"id": "example"}, "select 1", loader)]


def test_get_all_scopes_loads_scope_schemas(monkeypatch):
    loader = DatabaseSchemaLoader()
    scope_rows = [
        SimpleNamespace(id="scope-a", schema_data=json.dumps({"id": "SCOPE/A"})),
        SimpleNamespace(id="scope-b", schema_data=json.dumps({"id": "SCOPE/B"})),
    ]
    from_dict_calls = []

    monkeypatch.setattr(
        "schematools.contrib.django.loaders.Scope.objects",
        SimpleNamespace(all=lambda: scope_rows),
    )
    monkeypatch.setattr(
        "schematools.contrib.django.loaders.ScopeSchema.from_dict",
        lambda schema_dict: from_dict_calls.append(schema_dict) or schema_dict["id"],
    )

    scopes = loader._get_all_scopes()

    assert from_dict_calls == [{"id": "SCOPE/A"}, {"id": "SCOPE/B"}]
    assert scopes == {
        "scope-a": "SCOPE/A",
        "scope-b": "SCOPE/B",
    }
