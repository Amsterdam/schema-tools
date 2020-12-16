"""Extra fixtures for ``schematools.contrib.django``"""
import json
from pathlib import Path

import pytest

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
