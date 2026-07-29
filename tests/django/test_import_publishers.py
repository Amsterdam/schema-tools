from __future__ import annotations

from io import StringIO
from unittest.mock import Mock

import pytest
from django.core.management import call_command

from schematools.contrib.django import models
from schematools.contrib.django.management.commands import import_publishers
from schematools.types import Publisher as PublisherSchema


@pytest.mark.django_db
def test_import_publishers(here):
    path = here / "files/publishers/GLEBZ.json"
    args = [path]
    call_command("import_publishers", *args)
    assert models.Publisher.objects.count() == 1
    assert models.Publisher.objects.first().id == "glebz"


@pytest.mark.django_db
def test_import_publishers_from_url(here, monkeypatch):
    schema = PublisherSchema.from_file(here / "files/publishers/GLEBZ.json")

    class StubLoader:
        def get_all_publishers(self):
            return {schema.id: schema}

    monkeypatch.setattr(import_publishers, "get_schema_loader", lambda schema_url: StubLoader())

    stdout = StringIO()
    call_command("import_publishers", schema_url="https://example.com/schema", stdout=stdout)

    assert models.Publisher.objects.count() == 1
    assert models.Publisher.objects.first().id == "glebz"
    assert "Loading publishers from https://example.com/schema" in stdout.getvalue()
    assert "Imported publishers: 1" in stdout.getvalue()


@pytest.mark.django_db
def test_import_publishers_updates_existing_publisher(here):
    schema = PublisherSchema.from_file(here / "files/publishers/GLEBZ.json")
    command = import_publishers.Command(stdout=StringIO())
    publisher = Mock(spec=models.Publisher)
    publisher.save_for_schema.return_value = publisher

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(models.Publisher.objects, "get", lambda id: publisher)
        result = command._import(schema)

    assert result is publisher
    publisher.save_for_schema.assert_called_once_with(schema)
    assert "Updated Datateam Glebz" in command.stdout.getvalue()


@pytest.mark.django_db
def test_import_publishers_reports_no_new_publishers_when_import_is_noop(here, monkeypatch):
    command = import_publishers.Command(stdout=StringIO())
    monkeypatch.setattr(command, "import_from_files", lambda publisher_files: [])

    command.handle(publisher=[here / "files/publishers/GLEBZ.json"], schema_url="unused")

    assert "No new publishers imported." in command.stdout.getvalue()
