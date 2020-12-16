"""Extra fixtures for ``schematools.contrib.django``"""
import json
from pathlib import Path

import pytest

from schematools.contrib.django.auth_backend import RequestProfile
from schematools.contrib.django.models import Dataset, Profile

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


@pytest.fixture()
def brp_r_profile(brp_r_profile_schema) -> Profile:
    """The persistent database profile object based on a downlaoded schema definition."""
    return Profile.create_for_schema(brp_r_profile_schema)


@pytest.fixture()
def brp_schema_json(here) -> dict:
    """Fixture for the BRP dataset"""
    path = here / "files/brp.json"
    return json.loads(path.read_text())


@pytest.fixture()
def brp_dataset(brp_schema_json) -> Dataset:
    """Create a remote dataset."""
    return Dataset.objects.create(
        name="brp",
        schema_data=brp_schema_json,
        enable_db=False,
    )


@pytest.fixture()
def correct_auth_profile(rf, brp_r_profile):
    # Correct: both scope and mandatory query parameters in the request.
    correct_request = rf.get(
        "/",
        data={
            "postcode": "1234AB",
            "lastname": "Foobar",
        },
    )
    correct_request.is_authorized_for = lambda *scopes: "BRP/R" in set(scopes)
    return RequestProfile(correct_request)


@pytest.fixture()
def incorrect_auth_profile(rf, brp_r_profile):
    # Incorrect: no queries are given, so no access is given.
    incorrect_request = rf.get(
        "/",
        data={
            "postcode": "1234AB",
        },
    )
    incorrect_request.is_authorized_for = lambda *scopes: "BRP/R" in set(scopes)
    auth_profile = RequestProfile(incorrect_request)
    return auth_profile
