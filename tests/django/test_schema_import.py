from __future__ import annotations

import pytest
from django.core.management import call_command
from django.db import connection

from schematools.contrib.django import models


@pytest.mark.django_db
def test_import_schema(here):
    """Prove that dataset schema gets imported correctly"""
    hr_json_path = here / "files/datasets/hr.json"
    args = [hr_json_path]
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 1
    assert models.Dataset.objects.first().name == "hr"
    assert models.DatasetTable.objects.count() == 4
    assert models.DatasetTable.objects.filter(name="maatschappelijkeactiviteiten").count() == 1


@pytest.mark.django_db
def test_import_schema_twice(here):
    """Prove that importing a dataset schema twice does not fail"""
    verblijfsobjecten = here / "files/datasets/verblijfsobjecten.json"
    gebieden = here / "files/datasets/gebieden.json"
    hr_json_path = here / "files/datasets/hr.json"
    args = [hr_json_path, verblijfsobjecten, gebieden]
    call_command("import_schemas", *args)
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 3
    assert models.Dataset.objects.get(name="hr") is not None


@pytest.mark.django_db()
def test_import_schema_update_runs_migrations(here):
    """Prove that importing a dataset schema twice does not fail"""
    verblijfsobjecten = here / "files/datasets/verblijfsobjecten.json"
    gebieden = here / "files/datasets/gebieden.json"
    hr_json_path = here / "files/datasets/hr.json"
    call_command(
        "import_schemas",
        gebieden,
        verblijfsobjecten,
        hr_json_path,
        create_tables=1,
        create_views=1,
    )
    activiteiten_table = models.DatasetTable.objects.get(name="maatschappelijkeactiviteiten")
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = '{activiteiten_table.db_table}'
            """
        )
        columns = [col for row in cursor.fetchall() for col in row]
        assert "activiteit_type" not in columns

    updated_hr_json_path = here / "files/datasets/hr_updated.json"
    call_command("import_schemas", updated_hr_json_path, gebieden, verblijfsobjecten, verbosity=3)
    activiteiten_table = models.DatasetTable.objects.get(name="maatschappelijkeactiviteiten")
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = '{activiteiten_table.db_table}'
            """
        )
        columns = [col for row in cursor.fetchall() for col in row]
        assert "activiteit_type" in columns


@pytest.mark.django_db()
def test_import_schema_update_has_dry_run(here, capsys):
    """Prove that importing a dataset schema twice does not fail"""
    verblijfsobjecten = here / "files/datasets/verblijfsobjecten.json"
    gebieden = here / "files/datasets/gebieden.json"
    hr_json_path = here / "files/datasets/hr.json"
    call_command(
        "import_schemas",
        gebieden,
        verblijfsobjecten,
        hr_json_path,
        create_tables=1,
        create_views=1,
    )
    updated_hr_json_path = here / "files/datasets/hr_updated.json"
    call_command("import_schemas", updated_hr_json_path, gebieden, verblijfsobjecten, "--dry-run")
    activiteiten_table = models.DatasetTable.objects.get(name="maatschappelijkeactiviteiten")
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = '{activiteiten_table.db_table}'
            """
        )
        columns = [col for row in cursor.fetchall() for col in row]
        assert "activiteit_type" not in columns
    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out
    assert (
        """ALTER TABLE "hr_activiteiten_v1" ADD COLUMN "activiteit_type" varchar NULL;"""
        in captured.out
    )


@pytest.mark.django_db
def test_import_schema_enables_and_disables_api_based_on_status(here):
    """Prove that the enable_api flag is set at schema import time based on
    the 'status' field of he dataset.
        woonplaatsen has status: niet_beschikbaar
        hr has status: bechikbaar
    """
    hr_json_path = here / "files/datasets/hr.json"
    woonplaatsen_json_path = here / "files/datasets/woonplaatsen.json"
    args = [hr_json_path, woonplaatsen_json_path]
    call_command("import_schemas", *args)
    assert models.Dataset.objects.count() == 2
    assert models.Dataset.objects.get(name="hr").enable_api is True
    assert models.Dataset.objects.get(name="woonplaatsen").enable_api is False
