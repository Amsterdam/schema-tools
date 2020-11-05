import pytest
from unittest import mock
from django.test import RequestFactory
from schematools.contrib.django.auth_backend import (
    ProfileAuthorizationBackend,
    RequestProfile,
)
from schematools.contrib.django.models import Profile



@pytest.fixture
def profile_medewerker():
    return Profile.objects.create(
        name="medewerker", scopes="['FP/MD']", schema_data={"datasets": {}}
    )


@pytest.fixture
def profile_brk_read():
    return Profile.objects.create(
        name="brk_read",
        scopes="['BRK/RO']",
        schema_data={
            "datasets": {"brk": {"tables": {"geheim": {"fields": {"id": "encoded"}}}}}
        },
    )


@pytest.fixture
def profile_brk_readall():
    return Profile.objects.create(
        name="brk_readall",
        scopes="['BRK/RO','BRK/RSN']",
        schema_data={
            "datasets": {
                "brk": {"tables": {"geheim": {"fields": {"id": "read", "bsn": "read"}}}}
            }
        },
    )


@pytest.mark.django_db
def test_get_profiles_for_request(profile_medewerker, profile_brk_read):
    """Check that correct Profiles returned for request."""
    request = RequestFactory().get("/")
    request.is_authorized_for = lambda scopes: "FP/MD" in scopes

    request_profile = RequestProfile(request)

    assert request_profile.get_profiles() == {profile_medewerker}


@pytest.mark.django_db
def test_has_perm_dataset(profile_medewerker, profile_brk_read):
    """Check that BRK user can access data, while regular medeweker not."""
    request = RequestFactory().get("/")
    request.is_authorized_for = lambda scopes: True  # Both Profiles used

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk") is True


@pytest.mark.django_db
def test_has_perm_dataset_table(profile_medewerker, profile_brk_read):
    """Check that BRK user can access data, while regular medeweker not."""
    request = RequestFactory().get("/")
    request.is_authorized_for = lambda scopes: True  # Both Profiles used

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:geheim") is True


@pytest.mark.django_db
def test_has_perm_dataset_field(profile_medewerker, profile_brk_read):
    """Check that BRK user can access data, while regular medeweker not."""
    request = RequestFactory().get("/")
    request.is_authorized_for = lambda scopes: True  # Both Profiles used

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:geheim:id") is True
    assert backend.has_perm(user_1, "brk:geheim:bsn") is False


@pytest.mark.django_db
def test_has_perm_dataset_field_readall(profile_brk_readall):
    """Check that BRK user can access data, while regular medeweker not."""
    request = RequestFactory().get("/")
    request.is_authorized_for = lambda *scopes: True  # Both Profiles used

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:geheim:id") is True
    assert backend.has_perm(user_1, "brk:geheim:bsn") is True
