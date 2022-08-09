import pytest
from django.core.management import call_command

from schematools.contrib.django import models


@pytest.mark.django_db
def test_import_schema(here):
    """Prove that dataset schema gets imported correctly"""
    hr_json_path = here / "files" / "hr.json"
    args = [hr_json_path]
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 1
    assert models.Dataset.objects.first().name == "hr"
    assert models.DatasetTable.objects.count() == 4
    assert models.DatasetTable.objects.filter(name="maatschappelijkeactiviteiten").count() == 1


@pytest.mark.django_db
def test_import_schema_twice(here):
    """Prove that importing a dataset schema twice does not fail"""
    hr_json_path = here / "files" / "hr.json"
    args = [hr_json_path]
    call_command("import_schemas", *args)
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 1
    assert models.Dataset.objects.first().name == "hr"


@pytest.mark.django_db
def test_import_schema_enables_and_disables_api_based_on_status(here):
    """Prove that the enable_api flag is set at schema import time based on
    the 'status' field of he dataset.
        baggob has status: niet_beschikbaar
        hr has status: bechikbaar
    """
    hr_json_path = here / "files" / "hr.json"
    woonplaatsen_json_path = here / "files" / "woonplaatsen.json"
    args = [hr_json_path, woonplaatsen_json_path]
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 2
    assert models.Dataset.objects.get(name="hr").enable_api is True
    assert models.Dataset.objects.get(name="baggob").enable_api is False
