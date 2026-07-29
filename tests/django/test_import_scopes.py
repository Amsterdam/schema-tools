from __future__ import annotations

from io import StringIO
from unittest.mock import Mock

import pytest
from django.core.management import call_command

from schematools.contrib.django import models
from schematools.contrib.django.management.commands import import_scopes
from schematools.types import Scope as ScopeSchema


@pytest.mark.django_db
def test_import_scopes(here):
    path = here / "files/scopes/GLEBZ/glebzscope.json"
    args = [path]
    call_command("import_scopes", *args)
    assert models.Scope.objects.count() == 1
    assert models.Scope.objects.first().id == "glebz"


@pytest.mark.django_db
def test_import_scopes_from_url(here, monkeypatch):
    schema = ScopeSchema.from_file(here / "files/scopes/GLEBZ/glebzscope.json")

    class StubLoader:
        def get_all_scopes(self):
            return {schema.id: schema}

    monkeypatch.setattr(import_scopes, "get_schema_loader", lambda schema_url: StubLoader())

    stdout = StringIO()
    call_command("import_scopes", schema_url="https://example.com/schema", stdout=stdout)

    assert models.Scope.objects.count() == 1
    assert models.Scope.objects.first().id == "glebz"
    assert "Loading scopes from https://example.com/schema" in stdout.getvalue()
    assert "Imported scopes: 1" in stdout.getvalue()


@pytest.mark.django_db
def test_import_scopes_updates_existing_scope(here):
    schema = ScopeSchema.from_file(here / "files/scopes/GLEBZ/glebzscope.json")
    command = import_scopes.Command(stdout=StringIO())
    scope = Mock(spec=models.Scope)
    scope.save_for_schema.return_value = scope

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(models.Scope.objects, "get", lambda id: scope)
        result = command._import(schema)

    assert result is scope
    scope.save_for_schema.assert_called_once_with(schema)
    assert "Updated GLEBZscope" in command.stdout.getvalue()


@pytest.mark.django_db
def test_import_scopes_reports_no_new_scopes_when_import_is_noop(here, monkeypatch):
    command = import_scopes.Command(stdout=StringIO())
    monkeypatch.setattr(command, "import_from_files", lambda scope_files: [])

    command.handle(scope=[here / "files/scopes/GLEBZ/glebzscope.json"], schema_url="unused")

    assert "No new scopes imported." in command.stdout.getvalue()
