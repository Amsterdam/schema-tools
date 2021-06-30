import json
from pathlib import Path
from typing import Any

import pytest
from django.test import RequestFactory

from schematools.contrib.django.auth_backend import RequestProfile
from schematools.contrib.django.models import Dataset, Profile
from schematools.types import DatasetSchema, ProfileSchema

# Pytest decorators are untyped
# mypy: allow-untyped-decorators


@pytest.fixture
def profile_medewerker() -> Profile:
    """Fixture for medewerker profile."""
    return Profile.objects.create(
        name="medewerker", scopes="['FP/MD']", schema_data={"datasets": {}}
    )


@pytest.fixture
def profile_brk_read() -> Profile:
    """Fixture for brk read only profile."""
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
def profile_brk_read_full() -> Profile:
    """Fixture for brk full access profile."""
    return Profile.objects.create(
        name="brk_read_full",
        scopes="['BRK/RO','BRK/RSN']",
        schema_data={
            "datasets": {
                "brk": {
                    "tables": {
                        "kadastraleobjecten": {"fields": {"id": "read", "volgnummer": "read"}}
                    }
                }
            }
        },
    )


@pytest.fixture
def kadastralobjecten_schema_json(here: Path) -> Any:
    """Fixture for kadastraleobjecten schema."""
    path = here / "files" / "kadastraleobjecten.json"
    return json.loads(path.read_text())


@pytest.fixture
def kadastralobjecten_dataset(kadastralobjecten_schema_json: dict) -> Dataset:
    """Fixture for kadastraleobjecten dataset."""
    return Dataset.objects.create(
        name="brk", schema_data=kadastralobjecten_schema_json, path="brk"
    )


@pytest.fixture
def brp_r_profile(brp_r_profile_schema: ProfileSchema) -> Profile:
    """The persistent database profile object based on a downlaoded schema definition."""
    return Profile.create_for_schema(brp_r_profile_schema)


@pytest.fixture
def brp_schema_json(here: Path) -> Any:
    """Fixture for the BRP dataset."""
    path = here / "files/brp.json"
    return json.loads(path.read_text())


@pytest.fixture
def brp_dataset(brp_schema_json: dict) -> Dataset:
    """Create a remote dataset."""
    return Dataset.objects.create(
        name="brp", schema_data=brp_schema_json, enable_db=False, path="brp"
    )


@pytest.fixture
def correct_auth_profile(rf: RequestFactory, brp_r_profile: Profile) -> RequestProfile:
    """Fixture for corrrect auth profile request."""
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


@pytest.fixture
def incorrect_auth_profile(rf: RequestFactory, brp_r_profile: Profile) -> RequestProfile:
    """Fixture for incorrrect auth profile request."""
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


@pytest.fixture
def afval_dataset(afval_schema: DatasetSchema) -> Dataset:
    """Create Afvalwegingen dataset."""
    return Dataset.create_for_schema(afval_schema)


@pytest.fixture
def brk_dataset(brk_schema: DatasetSchema) -> Dataset:
    """Create full BRK virtual Databset."""
    dataset = Dataset.create_for_schema(brk_schema)
    dataset.enable_db = False
    dataset.save()
    return dataset


@pytest.fixture
def gebieden_dataset(gebieden_schema: DatasetSchema) -> Dataset:
    """Create gebieden dataset. DO NOT USE TOGETHER WITH ggwgebieden_dataset."""
    return Dataset.create_for_schema(gebieden_schema)


@pytest.fixture
def ggwgebieden_dataset(ggwgebieden_schema: DatasetSchema) -> Dataset:
    """Create ggwgebieden dataset. DO NOT USE TOGETHER WITH gebieden_dataset."""
    return Dataset.create_for_schema(ggwgebieden_schema)


@pytest.fixture
def hr_dataset(hr_schema: DatasetSchema) -> Dataset:
    """Create HR dataset."""
    return Dataset.create_for_schema(hr_schema)


@pytest.fixture
def meetbouten_dataset(meetbouten_schema: DatasetSchema) -> Dataset:
    """Create Meetbouten dataset."""
    return Dataset.create_for_schema(meetbouten_schema)


@pytest.fixture
def meldingen_dataset(meldingen_schema: DatasetSchema) -> Dataset:
    """Create Meldingen dataset."""
    return Dataset.create_for_schema(meldingen_schema)


@pytest.fixture
def parkeervakken_dataset(parkeervakken_schema: DatasetSchema) -> Dataset:
    """Create Parkeervakken dataset."""
    return Dataset.create_for_schema(parkeervakken_schema)


@pytest.fixture
def verblijfsobjecten_dataset(verblijfsobjecten_schema: DatasetSchema) -> Dataset:
    """Create Verblijfsobjecten dataset."""
    return Dataset.create_for_schema(verblijfsobjecten_schema)


@pytest.fixture
def woningbouwplannen_dataset(woningbouwplannen_schema: DatasetSchema) -> Dataset:
    """Create Woning Bouw Plannen dataset."""
    return Dataset.create_for_schema(woningbouwplannen_schema)
