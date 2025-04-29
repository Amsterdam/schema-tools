from __future__ import annotations

import pytest
from django.core.management import call_command
from django.db import connection

from schematools.contrib.django import models
from schematools.contrib.django.factories import model_factory


@pytest.mark.django_db
def test_remove_schema_and_tables(here):
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

    call_command("import_schemas", *args, create_tables=True, dry_run=False)
    assert models.Dataset.objects.count() == 4
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
        for table_schema in ds.schema.tables:
            assert model_factory(ds, table_schema).objects.count() == 0

    call_command("remove_schemas", "afvalwegingen", drop_tables=True)

    assert models.Dataset.objects.count() == 3
    assert models.DatasetTable.objects.count() == 9

    assert afvalweging_tables.isdisjoint(connection.introspection.table_names())

    # dont delete the tables
    call_command("remove_schemas", "parkeervakken")

    assert models.Dataset.objects.count() == 2
    assert models.DatasetTable.objects.count() == 7

    assert parkeervak_tables.issubset(connection.introspection.table_names())

    # Delete last two datasets
    call_command("remove_schemas", "verblijfsobjecten", "gebieden")

    assert models.Dataset.objects.count() == 0
