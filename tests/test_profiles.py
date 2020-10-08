import pytest
from schematools.contrib.django.models import Profile


@pytest.mark.django_db
def test_profile_permissions_loaded_correctly():
    """Prove that permissions loaded correctly"""
    profile = Profile.objects.create(
        name="test",
        scopes=["FP/MD"],
        schema_data={
            "datasets": {
                "parkeervakken": {
                    "permissions": "read"
                },
                "gebieden": {
                    "tables": {
                        "bouwblokken": {
                            "fields": {
                                "ligtInBuurt": "encoded"
                            }
                        }
                    }
                }
            }
        }
    )

    permissions = profile.get_permissions()

    assert permissions["parkeervakken"] == "read", repr(permissions)
    assert permissions["gebieden:bouwblokken:ligt_in_buurt"] == "encoded", repr(permissions)

