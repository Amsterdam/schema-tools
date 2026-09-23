from __future__ import annotations

from types import SimpleNamespace

from schematools.contrib.django.auth_backend import ProfileAuthorizationBackend


def test_has_perm_returns_true_for_accessible_field(monkeypatch):
    backend = ProfileAuthorizationBackend()
    field = object()
    request = SimpleNamespace(
        user_scopes=SimpleNamespace(has_field_access=lambda candidate: candidate is field)
    )
    user = SimpleNamespace(request=request)
    model = SimpleNamespace(table_schema=lambda: SimpleNamespace(fields={"field": field}))
    get_model_calls = []

    def fake_get_model(dataset_id, table_id):
        get_model_calls.append((dataset_id, table_id))
        return model

    monkeypatch.setattr("schematools.contrib.django.auth_backend.apps.get_model", fake_get_model)

    assert backend.has_perm(user, "dataset:table:field") is True
    assert get_model_calls == [("dataset", "table")]


def test_has_perm_returns_false_for_inaccessible_field(monkeypatch):
    backend = ProfileAuthorizationBackend()
    field = object()
    request = SimpleNamespace(user_scopes=SimpleNamespace(has_field_access=lambda _field: None))
    user = SimpleNamespace(request=request)
    model = SimpleNamespace(table_schema=lambda: SimpleNamespace(fields={"field": field}))

    monkeypatch.setattr(
        "schematools.contrib.django.auth_backend.apps.get_model",
        lambda _dataset_id, _table_id: model,
    )

    assert backend.has_perm(user, "dataset:table:field") is False
