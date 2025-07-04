from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.models import Dataset


class Command(BaseCommand):
    help = "Create the tables based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip",
            dest="skip",
            nargs="*",
            help="Schemas that need to be skipped",
        )

    def handle(self, *args, **options):
        skip = options.get("skip")
        create_tables(self, Dataset.objects.db_enabled(), allow_unmanaged=True, skip=skip)


def create_tables(
    command: BaseCommand,
    datasets: Iterable[Dataset],
    allow_unmanaged: bool = False,
    base_app_name: str | None = None,
    skip: list[str] | None = None,
    dry_run: bool = False,
) -> None:  # noqa: C901
    """Create tables for all updated datasets.
    This is a separate function to allow easy reuse.
    """
    errors = 0
    command.stdout.write("Creating tables")

    # First create all models. This allows Django to resolve  model relations.
    models = []
    to_be_skipped = set(skip if skip is not None else [])

    # Because datasets are related, we need to 'prewarm'
    # the datasets cache (the DatasetSchema.loader)
    # by accessing the `Dataset.schema` attribute.
    for dataset in datasets:
        dataset.schema  # noqa: B018

    for dataset in datasets:
        if not dataset.enable_db or dataset.name in to_be_skipped:
            continue  # in case create_tables() is called by import_schemas

        factory = DjangoModelFactory(dataset)
        models.extend(factory.build_models())

    # Grouping multiple versions of same model by table name
    models_by_table = defaultdict(list)
    for model in models:
        models_by_table[model._meta.db_table].append(model)

    # Create all tables
    with connection.schema_editor() as schema_editor:
        for db_table_name, models_group in models_by_table.items():
            # Only create tables if migration is allowed
            # - router allows it (not some external database)
            # - table is not a view
            # - model is managed (not by default)
            # - user overrides this (e.g. developer)
            # - create table for latest version of this dataset group
            model = max(models_group, key=lambda model: model._table_schema.version)

            is_view = False

            for view_table in model._dataset.schema.tables:
                if view_table.is_view:
                    command.stdout.write(f"  Skipping view: {db_table_name}")
                    is_view = True

            if is_view:
                continue

            router_allows = router.allow_migrate_model(model._meta.app_label, model)
            if not router_allows:
                command.stdout.write(f"  Skipping externally managed table: {db_table_name}")
                continue

            if not allow_unmanaged and not model._meta.can_migrate(connection):
                command.stderr.write(f"  Skipping non-managed model: {model._meta.db_table}")
                continue

            if dry_run:
                command.stdout.write(
                    f"* Would create table {model._meta.db_table} if it doesn't exist"
                )
            else:
                try:
                    with transaction.atomic():
                        schema_editor.create_model(model)
                except (DatabaseError, ValueError) as e:
                    command.stderr.write(f"  Cannot create table {model._meta.db_table}: {e}")
                    if not re.search(r'relation "[^"]+" already exists', str(e)):
                        errors += 1
                else:
                    command.stdout.write(f"* Created table {model._meta.db_table}")

    if errors:
        raise CommandError("Not all tables could be created")
