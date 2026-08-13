from __future__ import annotations

from datetime import timedelta
from io import StringIO
from types import SimpleNamespace

import pytest
from django.core.management import CommandError, call_command
from django.db import connection
from django.db.utils import ProgrammingError
from django.utils import timezone

from schematools.contrib.django import models
from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.management.commands import (
    delete_expired_schemas,
    soft_delete_schemas,
)


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


def test_soft_delete_schema_reports_unknown_schema_with_hint(monkeypatch):
    command = soft_delete_schemas.Command(stdout=StringIO())

    class DatasetQuerySet(list):
        def all(self):
            return self

    datasets = DatasetQuerySet([SimpleNamespace(name="my_dataset")])
    monkeypatch.setattr(soft_delete_schemas.Dataset.objects, "all", lambda: datasets)

    with pytest.raises(CommandError, match=r"did you mean 'my_dataset'\?"):
        command.handle(schemas=["myDataset"])


def test_delete_expired_schemas_reports_when_nothing_is_expired(monkeypatch):
    command = delete_expired_schemas.Command(stdout=StringIO())

    class EmptyQuerySet(list):
        def exists(self):
            return False

    monkeypatch.setattr(
        delete_expired_schemas.Dataset.objects,
        "filter",
        lambda **kwargs: EmptyQuerySet(),
    )

    command.handle(verbosity=1)

    assert command.stdout.getvalue().strip() == "No expired schemas found."


def test_delete_expired_schemas_reports_failed_table_drop(monkeypatch):
    command = delete_expired_schemas.Command(stdout=StringIO())
    deleted: list[str] = []
    executed = []
    dataset = SimpleNamespace(name="expired_dataset")
    dataset.delete = lambda: deleted.append(dataset.name)

    class ExpiredQuerySet(list):
        def exists(self):
            return True

    expired_datasets = ExpiredQuerySet([dataset])
    tables = [SimpleNamespace(db_table="expired_table")]

    class Cursor:
        def execute(self, statement):
            executed.append(str(statement))
            raise ProgrammingError("already gone")

    class CursorContext:
        def __enter__(self):
            return Cursor()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        delete_expired_schemas.Dataset.objects,
        "filter",
        lambda **kwargs: expired_datasets,
    )
    monkeypatch.setattr(
        delete_expired_schemas.DatasetTable.objects,
        "filter",
        lambda **kwargs: tables,
    )
    monkeypatch.setattr(delete_expired_schemas.connection, "cursor", lambda: CursorContext())

    command.handle(verbosity=1)

    assert len(executed) == 1
    assert "DROP TABLE" in executed[0]
    assert "expired_table" in executed[0]
    assert "CASCADE" in executed[0]
    assert deleted == ["expired_dataset"]
    assert command.stdout.getvalue().splitlines() == [
        "Failed to delete table expired_table. Error: already gone",
        "Deleted datasets expired_dataset",
    ]
