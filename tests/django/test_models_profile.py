import pytest

from schematools.contrib.django.models import Profile
from schematools.types import Permission, PermissionLevel


@pytest.mark.django_db
def test_profile(brp_r_profile_schema):
    """Prove that the BRP data is properly stored in the DB"""
    brp_r_profile = Profile.create_for_schema(brp_r_profile_schema)
    assert brp_r_profile.name == "brp_medewerker"
    assert brp_r_profile.get_scopes() == {"BRP/R"}

    perm = Permission(PermissionLevel.READ)
    assert brp_r_profile.schema.datasets["brp"].tables["ingeschrevenpersonen"].permissions == perm
