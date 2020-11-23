import json
import pytest
from pathlib import Path
from unittest import mock
from django.test import RequestFactory
from schematools.contrib.django import models
from schematools.contrib.django.auth_backend import (
    ProfileAuthorizationBackend,
    RequestProfile,
)
from schematools.contrib.django.models import Profile, Dataset

TEST_FILES_FOLDER = Path(__file__).parent.parent / "tests" / "files"


@pytest.fixture
def profile_medewerker():
    return Profile.objects.create(
        name="medewerker", scopes="['FP/MD']", schema_data={"datasets": {}}
    )


@pytest.fixture
def profile_brk_read():
    return Profile.objects.create(
        name="brk_read",
        scopes="['BRK/RO', 'ONLY/ENCODED']",
        schema_data={
            "datasets": {
                "brk": {
                    "tables": {
                        "kadastraleobjecten": {
                            "fields": {
                                "volgnummer": "encoded",
                                "identificatie": "encoded",
                            }
                        }
                    }
                }
            }
        },
    )


@pytest.fixture
def profile_brk_read_full():
    return Profile.objects.create(
        name="brk_read_full",
        scopes="['BRK/RO','BRK/RSN']",
        schema_data={
            "datasets": {
                "brk": {
                    "tables": {
                        "kadastraleobjecten": {
                            "fields": {"id": "read", "volgnummer": "read"}
                        }
                    }
                }
            }
        },
    )


@pytest.fixture()
def brk_schema_json() -> dict:
    path = TEST_FILES_FOLDER / "kadastraleobjecten.json"
    return json.loads(path.read_text())


@pytest.fixture()
def brk_dataset(brk_schema_json) -> Dataset:
    return Dataset.objects.create(name="brk", schema_data=brk_schema_json)


@pytest.mark.django_db
def test_get_profiles_for_request(profile_medewerker, profile_brk_read):
    """Check that correct Profiles returned for request."""
    request = RequestFactory().get("/")

    def auth(*scopes):
        for scope in scopes:
            if scope in ["FP/MD"]:
                return True
        return False

    request.is_authorized_for = auth
    request_profile = RequestProfile(request)
    assert request_profile.get_profiles() == {profile_medewerker}


@pytest.mark.django_db
def test_has_perm_dataset(profile_medewerker, profile_brk_read, brk_dataset):
    """Check that user who is authorized for all authorization checks can access data"""
    request = RequestFactory().get("/")

    def auth(*scopes):
        return True

    request.is_authorized_for = auth
    backend = ProfileAuthorizationBackend()
    user_1 = mock.MagicMock()
    user_1.request = request
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:id") is True


@pytest.mark.django_db
def test_has_perm_dataset_field_two_profiles(
    profile_medewerker, profile_brk_read, profile_brk_read_full, brk_dataset
):
    """Checks that profiles combine for a given user.
    And that he can access data accordingly, while regular medeweker not."""
    request = RequestFactory().get("/")

    def auth(*scopes):  # Both Profiles used
        return any(scope == "BRK/RO" for scope in scopes)

    request.is_authorized_for = auth

    models.Dataset.objects.filter(name="brk").update(auth="MAG/NIET")

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:kadastraleobjecten:id") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:identificatie") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:volgnummer") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:registratiedatum") is False


@pytest.mark.django_db
def test_has_perm_dataset_field_read_profile(profile_brk_read_full, brk_dataset):
    """Check that user with one profile can access data, while regular medeweker not."""
    request = RequestFactory().get("/")

    def auth(*scopes):  # Read Profile used
        return any(scope == "BRK/RSN" for scope in scopes)

    request.is_authorized_for = auth

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:kadastraleobjecten:id") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:volgnummer") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:identificatie") is False


@pytest.mark.django_db
def test_profile_field_inheritance_two_profiles(
    rf, profile_brk_read, profile_brk_read_full, brk_dataset
):
    """Tests that profiles field permissions inherit properly"""

    def auth(*scopes):  # Both Profiles used
        return any(scope == "BRK/RO" for scope in scopes)

    models.Dataset.objects.filter(name="brk").update(auth="MAG/NIET")

    request = rf.get("/")
    request.auth_profile = RequestProfile(request)
    request.is_authorized_for = auth
    auth_profile = request.auth_profile
    assert auth_profile.get_read_permission(perm="brk:kadastraleobjecten:id") == "read"
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:volgnummer")
        == "read"
    )
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:identificatie")
        == "encoded"
    )
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:registratiedatum")
        is None
    )


@pytest.mark.django_db
def test_profile_field_inheritance_from_dataset(
    rf, profile_brk_read, profile_brk_read_full, brk_dataset
):
    """Tests dataset permissions overrule lower profile permissions"""

    def auth(*scopes):  # 1 profile and dataset scope
        return bool({"ONLY/ENCODED", "DATASET/SCOPE"}.intersection(scopes))

    models.Dataset.objects.filter(name="brk").update(auth="DATASET/SCOPE")

    request = rf.get("/")
    request.auth_profile = RequestProfile(request)
    request.is_authorized_for = auth
    auth_profile = request.auth_profile
    assert auth_profile.get_read_permission(perm="brk:kadastraleobjecten:id") == "read"
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:volgnummer")
        == "read"
    )
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:identificatie")
        == "read"
    )
    assert (
        auth_profile.get_read_permission(perm="brk:kadastraleobjecten:registratiedatum")
        == "read"
    )
