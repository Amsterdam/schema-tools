import re
from collections import defaultdict
from typing import Iterable, List, Optional

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset


class Command(BaseCommand):
    help = "Create the views based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):
        create_views(self, Dataset.objects.db_enabled())


def create_views(
    command: BaseCommand,
    datasets: Iterable[Dataset],
    base_app_name: Optional[str] = None,
) -> None:  # noqa: C901
    """Create tables for all updated datasets.
    This is a separate function to allow easy reuse.
    """
    errors = 0
    command.stdout.write("Creating tables")

    # First create all models. This allows Django to resolve  model relations.
    models = []

    # Because datasets are related, we need to 'prewarm'
    # the datasets cache (the DatasetSchema.dataset_collection)
    # by accessing the `Dataset.schema` attribute.
    for dataset in datasets:
        dataset.schema

    for dataset in datasets:
        if not dataset.enable_db:
            continue  # in case create_tables() is called by import_schemas

        models.extend(schema_models_factory(dataset, base_app_name=base_app_name))

    # Grouping multiple versions of same model by table name
    models_by_table = defaultdict(list)
    for model in models:
        models_by_table[model._meta.db_table].append(model)

    # Create all tables
    with connection.schema_editor() as schema_editor:
        for db_table_name, models_group in models_by_table.items():
            model = max(models_group, key=lambda model: model._dataset.version)
            if model.is_view():
                try:
                    command.stdout.write(f"* Creating view {model._meta.db_table}")
                    with transaction.atomic():
                        schema_editor.create_model(model)
                except (DatabaseError, ValueError) as e:
                    command.stderr.write(f"  Tables not created: {e}")
                    if not re.search(r'relation "[^"]+" already exists', str(e)):
                        errors += 1

    if errors:
        raise CommandError("Not all tables could be created")
