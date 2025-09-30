from __future__ import annotations

import pytest
from django.core.management import call_command
from django.db import connection

from schematools.contrib.django import models
from schematools.contrib.django.management.commands.import_schemas import Command


@pytest.mark.django_db
def test_import_schema(here):
    """Prove that dataset schema gets imported correctly"""
    hr_json_path = here / "files/datasets/hr.json"
    args = [hr_json_path]
    call_command("import_schemas", *args, dry_run=False)
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
    call_command("import_schemas", *args, dry_run=False)
    call_command("import_schemas", *args, dry_run=False)
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
        dry_run=False,
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
    call_command(
        "import_schemas",
        updated_hr_json_path,
        gebieden,
        verblijfsobjecten,
        verbosity=3,
        dry_run=False,
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
        dry_run=False,
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


@pytest.mark.django_db()
def test_import_schema_with_table_migrations(here, capsys):
    """Prove that importing a dataset schema with migrate tables runs migrations"""
    verblijfsobjecten = here / "files/datasets/verblijfsobjecten.json"
    gebieden = here / "files/datasets/gebieden.json"
    hr_json_path = here / "files/datasets/hr.json"
    call_command(
        "import_schemas",
        hr_json_path,
        gebieden,
        verblijfsobjecten,
        "--execute",
        "--create-tables",
        "--migrate-tables",
    )
    updated_hr_json_path = here / "files/datasets/hr_updated.json"
    call_command(
        "import_schemas",
        updated_hr_json_path,
        gebieden,
        verblijfsobjecten,
        "--execute",
        "--migrate-tables",
    )
    captured = capsys.readouterr()
    assert """* Processing table maatschappelijkeactiviteiten""" in captured.out


@pytest.mark.django_db()
def test_import_schema_with_no_table_migrations(here, capsys):
    """Prove that importing a dataset schema with migrate tables runs migrations"""
    verblijfsobjecten = here / "files/datasets/verblijfsobjecten.json"
    gebieden = here / "files/datasets/gebieden.json"
    hr_json_path = here / "files/datasets/hr.json"
    call_command(
        "import_schemas",
        hr_json_path,
        gebieden,
        verblijfsobjecten,
        "--execute",
        "--no-migrate-tables",
    )
    updated_hr_json_path = here / "files/datasets/hr_updated.json"
    call_command(
        "import_schemas",
        updated_hr_json_path,
        gebieden,
        verblijfsobjecten,
        "--execute",
        "--no-migrate-tables",
    )
    captured = capsys.readouterr()
    assert """* Processing table maatschappelijkeactiviteiten""" not in captured.out


@pytest.mark.django_db()
def test_import_schema_does_not_alter_column(here, capsys):
    """Prove that ALTER COLUMN statements are filtered from generated SQL"""
    gebieden = here / "files/datasets/gebieden.json"
    call_command(
        "import_schemas",
        gebieden,
        create_tables=1,
        create_views=1,
        dry_run=False,
    )
    updated_gebieden_json_path = here / "files/datasets/gebieden_updated_comment.json"
    call_command("import_schemas", updated_gebieden_json_path, "--dry-run")
    captured = capsys.readouterr()
    assert (
        """ALTER TABLE "gebieden_bouwblokken_v1" ALTER COLUMN "identificatie" TYPE varchar;"""
        not in captured.out
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
    call_command("import_schemas", *args, dry_run=False)
    assert models.Dataset.objects.count() == 2
    assert models.Dataset.objects.get(name="hr").enable_api is True
    assert models.Dataset.objects.get(name="woonplaatsen").enable_api is False


@pytest.mark.django_db
def test_import_schema_drops_experimental_table_with_breaking_change(here):
    original = here / "files/datasets/experimental/original.json"
    call_command("import_schemas", original, dry_run=False, create_tables=1)

    assert models.Dataset.objects.count() == 1
    table = models.DatasetTable.objects.get(name="experimentaltable")
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = '{table.db_table}'
            """
        )
        columns = [col for row in cursor.fetchall() for col in row]
        assert "other" in columns

    updated = here / "files/datasets/experimental/removed_field.json"
    call_command("import_schemas", updated, dry_run=False, create_tables=1)

    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = '{table.db_table}'
            """
        )
        columns = [col for row in cursor.fetchall() for col in row]
        assert "other" not in columns


@pytest.mark.django_db
def test_import_schema_drops_experimental_table_with_breaking_change_no_create_tables(
    here, capsys
):
    original = here / "files/datasets/experimental/original.json"
    call_command("import_schemas", original, dry_run=False, create_tables=1)
    assert models.Dataset.objects.count() == 1

    updated = here / "files/datasets/experimental/removed_field.json"
    call_command("import_schemas", updated, dry_run=False, create_tables=False)
    captured = capsys.readouterr()
    assert """Not dropping table, as create_tables is set to false.""" in captured.out


@pytest.mark.django_db
def test_import_schema_drops_experimental_table_with_breaking_change_dry_run(here, capsys):
    original = here / "files/datasets/experimental/original.json"
    call_command("import_schemas", original, dry_run=False, create_tables=1)
    assert models.Dataset.objects.count() == 1

    updated = here / "files/datasets/experimental/removed_field.json"
    call_command("import_schemas", updated, dry_run=True, create_tables=1)
    captured = capsys.readouterr()
    assert """Would drop and replace table experimental_experimentaltable_v1.""" in captured.out


@pytest.mark.django_db
def test_import_schema_doesnt_drop_experimental_table_with_non_breaking_change_dry_run(
    here, capsys
):
    original = here / "files/datasets/experimental/original.json"
    call_command("import_schemas", original, dry_run=False, create_tables=1)
    assert models.Dataset.objects.count() == 1

    updated = here / "files/datasets/experimental/new_field.json"
    call_command("import_schemas", updated, dry_run=True, create_tables=1)
    captured = capsys.readouterr()
    assert (
        """ALTER TABLE "experimental_experimentaltable_v1" ADD COLUMN "another" bigint NULL;"""
        in captured.out
    )


@pytest.mark.django_db
def test_import_schema_drop_experimental_table_with_m2m_also_drops_through_table(here, capsys):
    original = here / "files/datasets/experimental/original_m2m.json"
    call_command("import_schemas", original, dry_run=False, create_tables=1)
    assert models.Dataset.objects.count() == 1

    # There's a through table
    with connection.cursor() as cursor:
        cursor.execute("""SELECT table_name FROM information_schema.tables""")
        tables = [col for row in cursor.fetchall() for col in row]
        assert "experimental_experimentaltable_ligt_in_other_table_v1" in tables

    updated = here / "files/datasets/experimental/removed_field_m2m.json"
    call_command("import_schemas", updated, dry_run=False, create_tables=1)

    # Through table has been deleted
    with connection.cursor() as cursor:
        cursor.execute("""SELECT table_name FROM information_schema.tables""")
        tables = [col for row in cursor.fetchall() for col in row]
        assert "experimental_experimentaltable_ligt_in_other_table_v1" not in tables


@pytest.mark.django_db
def test_missing_datasets_if_match(
    gebieden_dataset,
    verblijfsobjecten_dataset,
    verblijfsobjecten_schema,
    gebieden_schema,
):
    """Prove that missing_datasets is empty when datasets match"""
    command = Command()

    current = {"verblijfsobjecten": verblijfsobjecten_dataset, "gebieden": gebieden_dataset}
    updated = {"verblijfsobjecten": verblijfsobjecten_schema, "gebieden": gebieden_schema}

    missing_datasets = command.get_missing_datasets(current, updated)

    assert missing_datasets == []


@pytest.mark.django_db
def test_missing_datasets(
    gebieden_dataset,
    verblijfsobjecten_dataset,
    hr_dataset,
    verblijfsobjecten_schema,
    gebieden_schema,
):
    """Prove that missing_datasets returns the correct dataset"""
    command = Command()

    current = {
        "verblijfsobjecten": verblijfsobjecten_dataset,
        "gebieden": gebieden_dataset,
        "hr": hr_dataset,
    }
    updated = {"verblijfsobjecten": verblijfsobjecten_schema, "gebieden": gebieden_schema}

    missing_datasets = command.get_missing_datasets(current, updated)

    assert missing_datasets == [hr_dataset]


@pytest.mark.django_db
def test_missing_datasets_import(here, dataset_library, capsys):
    """Prove that missing datasets get deleted by import_schemas command"""

    # Pass only two schemas
    gebieden = here / "files/datasets/gebieden.json"
    afval = here / "files/datasets/afval.json"
    args = [gebieden, afval]

    call_command("import_schemas", *args, dry_run=False)

    captured = capsys.readouterr()
    assert """Deleted the following datasets: {'parkeervakken'}""" in captured.out
    assert models.Dataset.objects.count() == 2
    assert not models.Dataset.objects.filter(name="parkeervakken").exists()
