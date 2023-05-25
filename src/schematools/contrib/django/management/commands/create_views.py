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
    """Create views. This is a separate function to allow easy reuse."""
    errors = 0
    command.stdout.write("Creating views")

    # Because datasets are related, we need to 'prewarm'
    # the datasets cache (the DatasetSchema.dataset_collection)
    # by accessing the `Dataset.schema` attribute.
    for dataset in datasets:
        dataset.schema

    for dataset in datasets:
        if not dataset.enable_db:
            continue  # in case create_tables() is called by import_schemas

    import pdb

    pdb.set_trace()
    # Create all views
    for dataset in datasets:
        for table in dataset.schema.tables:
            if table.is_view():
                """
                view_sql = table.view_sql
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SET ROLE write_{table.schema_name}")
                        cursor.execute(view_sql)
                        command.stdout.write(f"* Creating view {table.name}")
                except (DatabaseError, ValueError) as e:
                    command.stderr.write(f"  Views not created: {e}")
                    errors += 1
                """
    if errors:
        raise CommandError("Not all tables could be created")
