import re
from collections import defaultdict
from typing import Iterable, List, Optional, Type

from django.core.management import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS, DatabaseError, connection, router, transaction
from django_db_comments.db_comments import (
    add_column_comments_to_database,
    add_table_comments_to_database,
    get_comments_for_model,
)

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset, DynamicModel


class Command(BaseCommand):
    help = "Create the tables based on the uploaded Amsterdam schema's."
    requires_system_checks = False  # don't test URLs (which create models)

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


def _add_comments_to_database(used_models: List[DynamicModel]) -> None:
    """Add comments for tables and fields to the database.

    The code below is coming from the `django_db_comments` package.
    https://pypi.org/project/django-db-comments/

    Because we are creating models dynamically, the standard approach that is
    used (a `post_migrate` signal) in this package, does not apply in our case.
    So, we re-use the packages' code as much as possible in our own approach.
    """
    columns_comments = {m._meta.db_table: get_comments_for_model(m) for m in used_models}
    if columns_comments:
        add_column_comments_to_database(columns_comments, DEFAULT_DB_ALIAS)
    table_comments = {
        m._meta.db_table: m._meta.verbose_name.title() for m in used_models if m._meta.verbose_name
    }

    if table_comments:
        add_table_comments_to_database(table_comments, DEFAULT_DB_ALIAS)


def create_tables(
    command: BaseCommand,
    datasets: Iterable[Dataset],
    allow_unmanaged: bool = False,
    base_app_name: Optional[str] = None,
    skip: Optional[List[str]] = None,
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
    # the datasets cache (the DatasetSchema.dataset_collection)
    # by accessing the `Dataset.schema` attribute.
    for dataset in datasets:
        dataset.schema

    for dataset in datasets:
        if not dataset.enable_db or dataset.name in to_be_skipped:
            continue  # in case create_tables() is called by import_schemas

        models.extend(schema_models_factory(dataset, base_app_name=base_app_name))

    # We need to collect the models for later re-use in adding comments to db.
    used_models = []
    # Grouping multiple versions of same model by table name
    models_by_table = defaultdict(list)
    for model in models:
        models_by_table[model._meta.db_table].append(model)

    # Create all tables
    with connection.schema_editor() as schema_editor:
        for db_table_name, models_group in models_by_table.items():
            # Only create tables if migration is allowed
            # - router allows it (not some external database)
            # - model is managed (not by default)
            # - user overrides this (e.g. developer)
            # - create table for latest version of this dataset group
            model = max(models_group, key=lambda model: model._dataset.version)

            router_allows = router.allow_migrate_model(model._meta.app_label, model)
            if not router_allows:
                command.stdout.write(f"  Skipping externally managed table: {db_table_name}")
                continue

            if not allow_unmanaged and not model._meta.can_migrate(connection):
                command.stderr.write(f"  Skipping non-managed model: {model._meta.db_table}")
                continue

            try:
                command.stdout.write(f"* Creating table {model._meta.db_table}")
                with transaction.atomic():
                    used_models.append(model)
                    schema_editor.create_model(model)
            except (DatabaseError, ValueError) as e:
                command.stderr.write(f"  Tables not created: {e}")
                if not re.search(r'relation "[^"]+" already exists', str(e)):
                    errors += 1

    # Add the comments for tables/fields for the models that have been created.
    _add_comments_to_database(used_models)

    if errors:
        raise CommandError("Not all tables could be created")
