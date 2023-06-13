import re
from collections import defaultdict
from typing import Iterable, List, Optional
import requests

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset, DatasetTableSchema


class Command(BaseCommand):
    help = "Create the views based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):
        create_views(self, Dataset.objects.db_enabled())


def _get_view_sql(table: DatasetTableSchema) -> str:
    """Load a view SQL file from a URL."""
    url = table.view_url
    if not url.startswith("http"):
        raise ValueError(f"View URL {url} is not a valid URL")

    response = requests.get(url)

    try:
        response = requests.get(url, timeout=30)
    except requests.exceptions.Timeout:
        print(f"Timeout while loading view SQL from {url}")
    except requests.exceptions.TooManyRedirects:
        print(f"Too many redirects while loading view SQL from {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error while loading view SQL from {url}: {e}")

    if response.status_code == 200:
        return response.text
    else:
        print(f"Error while loading view SQL from {url}")
        raise ValueError(f"Could not load view SQL from {url}: {response.status_code}")


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

    # Create all views
    for dataset in datasets:
        for table in dataset.schema.tables:
            if table.is_view:
                view_sql = _get_view_sql(table)
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SET ROLE write_{table._parent_schema.db_name}")
                        cursor.execute(view_sql)
                        command.stdout.write(f"* Creating view {table.id}")
                except (DatabaseError, ValueError) as e:
                    command.stderr.write(f"  Views not created: {e}")
                    errors += 1

    if errors:
        raise CommandError("Not all tables could be created")
