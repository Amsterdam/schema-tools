from unittest import mock

import pytest

from schematools.contrib.django import models
from schematools.contrib.django.auth_backend import (
    ProfileAuthorizationBackend,
    RequestProfile,
)


@pytest.mark.django_db
def test_get_profiles_for_request(rf, profile_medewerker, profile_brk_read):
    """Check that correct Profiles returned for request."""
    request = rf.get("/")

    request.is_authorized_for = lambda *scopes: "FP/MD" in set(scopes)
    request_profile = RequestProfile(request)
    assert request_profile.get_profiles() == {profile_medewerker}


@pytest.mark.django_db
def test_active_profiles_invalid(incorrect_auth_profile):
    """Prove that only having a scope won't grant access."""
    active = incorrect_auth_profile.get_active_profiles("brp", "ingeschrevenpersonen")
    assert active == []


@pytest.mark.django_db
def test_active_profiles_valid(correct_auth_profile, brp_r_profile):
    """Prove that the right combination of filters activates the profile"""
    active = correct_auth_profile.get_active_profiles("brp", "ingeschrevenpersonen")
    assert active == [brp_r_profile]


@pytest.mark.django_db
def test_get_all_permissions(correct_auth_profile, brp_dataset):
    """Prove that all permissions can be retrieved"""
    permissions = correct_auth_profile.get_all_permissions(
        "brp:ingeschrevenpersonen:postcode"
    )
    assert permissions == {"brp:ingeschrevenpersonen:bsn": "encoded"}


@pytest.mark.django_db
def test_has_perm_dataset(rf, profile_medewerker, profile_brk_read, brk_dataset):
    """Check that user who is authorized for all authorization checks can access data"""
    request = rf.get("/")

    request.is_authorized_for = lambda *scopes: True
    backend = ProfileAuthorizationBackend()
    user_1 = mock.MagicMock()
    user_1.request = request
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:id") is True


@pytest.mark.django_db
def test_has_perm_dataset_field_two_profiles(
    rf, profile_medewerker, profile_brk_read, profile_brk_read_full, brk_dataset
):
    """Checks that profiles combine for a given user.
    And that he can access data accordingly, while regular medeweker not."""
    request = rf.get("/")

    # Both Profiles used
    request.is_authorized_for = lambda *scopes: "BRK/RO" in set(scopes)

    models.Dataset.objects.filter(name="brk").update(auth="MAG/NIET")

    backend = ProfileAuthorizationBackend()

    user_1 = mock.MagicMock()
    user_1.request = request

    assert backend.has_perm(user_1, "brk:kadastraleobjecten:id") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:identificatie") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:volgnummer") is True
    assert backend.has_perm(user_1, "brk:kadastraleobjecten:registratiedatum") is False


@pytest.mark.django_db
def test_has_perm_dataset_field_read_profile(rf, profile_brk_read_full, brk_dataset):
    """Check that user with one profile can access data, while regular medeweker not."""
    request = rf.get("/")

    # Read Profile used
    request.is_authorized_for = lambda *scopes: "BRK/RSN" in set(scopes)

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

    models.Dataset.objects.filter(name="brk").update(auth="MAG/NIET")

    request = rf.get("/")
    request.auth_profile = RequestProfile(request)
    request.is_authorized_for = lambda *scopes: "BRK/RO" in set(scopes)
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
    request.is_authorized_for = lambda *scopes: {"ONLY/ENCODED", "DATASET/SCOPE"} & set(
        scopes
    )
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
