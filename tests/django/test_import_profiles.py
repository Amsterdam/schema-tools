from __future__ import annotations

from io import StringIO
from unittest.mock import Mock

import pytest
from django.conf import settings
from django.core.management import call_command

from schematools.contrib.django import models
from schematools.contrib.django.management.commands import import_profiles
from schematools.types import ProfileSchema


@pytest.mark.django_db
def test_import_profiles(here, monkeypatch):
    path = here / "files/profiles/BRP_RNAME.json"
    monkeypatch.setattr(settings, "PROFILES_URL", "https://example.com/profiles", raising=False)
    call_command("import_profiles", path)

    assert models.Profile.objects.count() == 1
    assert models.Profile.objects.first().name == "brp_medewerker"


@pytest.mark.django_db
def test_import_profiles_from_url(here, monkeypatch):
    schema = ProfileSchema.from_file(here / "files/profiles/BRP_RNAME.json")
    monkeypatch.setattr(settings, "PROFILES_URL", "https://example.com/profiles", raising=False)

    class StubLoader:
        def get_all_profiles(self):
            return [schema]

    monkeypatch.setattr(import_profiles, "get_profile_loader", lambda schema_url: StubLoader())

    stdout = StringIO()
    call_command("import_profiles", schema_url="https://example.com/profiles", stdout=stdout)

    assert models.Profile.objects.count() == 1
    assert models.Profile.objects.first().name == "brp_medewerker"
    assert "Loading profiles from https://example.com/profiles" in stdout.getvalue()
    assert "Imported profiles: 1" in stdout.getvalue()


@pytest.mark.django_db
def test_import_profiles_updates_existing_profile(here):
    schema = ProfileSchema.from_file(here / "files/profiles/BRP_RNAME.json")
    command = import_profiles.Command(stdout=StringIO())
    profile = Mock(spec=models.Profile)
    profile.save_for_schema.return_value = profile

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(models.Profile.objects, "get", lambda name: profile)
        result = command._import(schema)

    assert result is profile
    profile.save_for_schema.assert_called_once_with(schema)
    assert "Updated brp_medewerker" in command.stdout.getvalue()


@pytest.mark.django_db
def test_import_profiles_reports_no_new_profiles_when_import_is_noop(here, monkeypatch):
    command = import_profiles.Command(stdout=StringIO())
    monkeypatch.setattr(command, "import_from_files", lambda profile_files: [])

    command.handle(profile=[here / "files/profiles/BRP_RNAME.json"], schema_url="unused")

    assert "No new profiles imported." in command.stdout.getvalue()
