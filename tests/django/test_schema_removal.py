from __future__ import annotations

from datetime import timedelta

import pytest
from django.core.management import call_command
from django.db import connection
from django.utils import timezone

from schematools.contrib.django import models
from schematools.contrib.django.factories import DjangoModelFactory


@pytest.mark.django_db
def test_soft_delete_schema_and_tables(here, capsys):
    """Prove that dataset schema gets imported correctly"""
    afval_json_path = here / "files/datasets/afvalwegingen.json"
    parkeervakken_json_path = here / "files/datasets/parkeervakken.json"
    verblijfsobjecten_json_path = here / "files/datasets/verblijfsobjecten.json"
    gebieden_json_path = here / "files/datasets/gebieden.json"
    args = [
        afval_json_path,
        parkeervakken_json_path,
        verblijfsobjecten_json_path,
        gebieden_json_path,
    ]

    call_command("import_schemas", *args, create_tables=True)
    assert models.Dataset.objects.count() == 4
    assert models.DatasetVersion.objects.count() == 4
    assert models.DatasetTable.objects.count() == 13

    parkeervak_tables = set(
        models.DatasetTable.objects.filter(dataset__name="parkeervakken").values_list(
            "db_table", flat=True
        )
    )
    afvalweging_tables = set(
        models.DatasetTable.objects.filter(dataset__name="afvalwegingen").values_list(
            "db_table", flat=True
        )
    )
    for ds in models.Dataset.objects.all():
        factory = DjangoModelFactory(ds)
        for table_schema in ds.schema.tables:
            assert factory.build_model(table_schema).objects.count() == 0

    # Add delete_date to dataset and tables
    call_command("soft_delete_schemas", "afvalwegingen", "parkeervakken")

    assert models.Dataset.objects.count() == 4
    assert models.DatasetTable.objects.count() == 13

    # Prove that datasets_datasetsversion table is not updated
    assert models.DatasetVersion.objects.count() == 4

    assert not afvalweging_tables.isdisjoint(connection.introspection.table_names())
    assert parkeervak_tables.issubset(connection.introspection.table_names())

    captured = capsys.readouterr()
    assert "Added delete date to table afvalwegingen_clusters_v1" in captured.out
    assert "Added delete date to table parkeervakken_parkeervakken_v1" in captured.out
    assert "Added delete date to dataset parkeervakken" in captured.out
    assert "Added delete date to dataset afvalwegingen" in captured.out


@pytest.mark.django_db
def test_hard_delete_schema_and_tables(here, capsys):
    """Prove that dataset schema gets imported correctly"""
    verblijfsobjecten_json_path = here / "files/datasets/verblijfsobjecten.json"
    gebieden_json_path = here / "files/datasets/gebieden.json"
    args = [
        verblijfsobjecten_json_path,
        gebieden_json_path,
    ]

    call_command("import_schemas", *args, create_tables=True)
    assert models.Dataset.objects.count() == 2
    assert models.DatasetVersion.objects.count() == 2
    assert models.DatasetTable.objects.count() == 7

    for ds in models.Dataset.objects.all():
        factory = DjangoModelFactory(ds)
        for table_schema in ds.schema.tables:
            assert factory.build_model(table_schema).objects.count() == 0

    # Add delete_date to dataset and tables
    call_command("soft_delete_schemas", "verblijfsobjecten", "gebieden")

    captured = capsys.readouterr()
    assert "Added delete date to dataset gebieden" in captured.out
    assert "Added delete date to table gebieden_buurten_v1" in captured.out
    assert "Added delete date to table verblijfsobjecten_verblijfsobjecten_v1" in captured.out
    assert "Added delete date to dataset verblijfsobjecten" in captured.out

    # set delete_date for dataset and tables to cutoff dates

    cutoff = timezone.now() - timedelta(days=30)
    lt_cutoff = timezone.now() - timedelta(days=14)

    models.Dataset.objects.filter(name="verblijfsobjecten").update(delete_date=cutoff)
    models.Dataset.objects.filter(name="gebieden").update(delete_date=lt_cutoff)
    vbo_tables = models.DatasetTable.objects.filter(db_table="verblijfsobjecten")
    geb_tables = models.DatasetTable.objects.filter(db_table="gebieden")

    for table in vbo_tables:
        table.delete_date = cutoff
        table.save(update_fields=["delete_date"])

    for table in geb_tables:
        table.delete_date = lt_cutoff
        table.save(update_fields=["delete_date"])

    call_command("delete_expired_schemas")
    assert models.Dataset.objects.count() == 1
    assert models.DatasetVersion.objects.count() == 1

    captured = capsys.readouterr()
    assert "Deleted datasets verblijfsobjecten" in captured.out
    assert "verblijfsobjecten_verblijfsobjecten_v1" in captured.out
