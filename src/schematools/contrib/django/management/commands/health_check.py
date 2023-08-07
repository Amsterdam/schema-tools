import re
from collections import defaultdict
from typing import Iterable, List, Optional

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset
from schematools.naming import to_snake_case

import requests

class Command(BaseCommand):
    help = "Check if any of the datasets is returning a 500 response."
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
        check_endpoints(self, Dataset.objects.db_enabled(), skip=skip)

def generate_url(enviroment: str, dataset: str, table: str):
    url = f"https://{enviroment}.api.data.amsterdam.nl/v1/{dataset}/{to_snake_case(table)}/"
    return url

def check_endpoints(
    command: BaseCommand,
    datasets: Iterable[Dataset],
    skip: Optional[List[str]] = None,
) -> None:
    """Check if all endpoints are reachable."""

    errors = []

    command.stdout.write("Checking endpoints")

    to_be_skipped = set(skip if skip is not None else [])

    for dataset in datasets:
        if not dataset.enable_db or dataset.name in to_be_skipped or dataset.schema.status.value == "niet_beschikbaar":
            continue  # in case create_tables() is called by import_schemas

        tables = dataset.schema.get_tables()
        for table in tables:
            url = generate_url("acc", dataset.name, table.id)
            command.stdout.write(f"Checking {url}")

            try:   
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    command.stdout.write("  (200) OK")
                    continue
                elif response.status_code == 500:
                    command.stdout.write("  (500) ERROR")
                elif response.status_code == 404:
                    command.stdout.write("  (404) NOT FOUND")
                elif response.status_code == 403:
                    command.stdout.write("  (403) FORBIDDEN")
                elif response.status_code == 401:
                    command.stdout.write("  (401) UNAUTHORIZED")
                errors.append({'url': url, 'status_code': response.status_code, 'dataset': dataset.name, 'table': table.id})

            except requests.exceptions.RequestException as e:
                command.stdout.write(f"  ERROR ({e}))")

    if errors:
        print("Errors:")
        for error in errors:
            command.stdout.write(f"  {error}")


# def create_tables(
#     command: BaseCommand,
#     datasets: Iterable[Dataset],
#     allow_unmanaged: bool = False,
#     base_app_name: Optional[str] = None,
#     skip: Optional[List[str]] = None,
# ) -> None:  # noqa: C901
#     """Create tables for all updated datasets.
#     This is a separate function to allow easy reuse.
#     """
#     errors = 0
#     command.stdout.write("Creating tables")

#     # First create all models. This allows Django to resolve  model relations.
#     models = []
#     to_be_skipped = set(skip if skip is not None else [])

#     # Because datasets are related, we need to 'prewarm'
#     # the datasets cache (the DatasetSchema.dataset_collection)
#     # by accessing the `Dataset.schema` attribute.
#     for dataset in datasets:
#         dataset.schema

#     for dataset in datasets:
#         if not dataset.enable_db or dataset.name in to_be_skipped:
#             continue  # in case create_tables() is called by import_schemas

#         models.extend(schema_models_factory(dataset, base_app_name=base_app_name))

#     # Grouping multiple versions of same model by table name
#     models_by_table = defaultdict(list)
#     for model in models:
#         models_by_table[model._meta.db_table].append(model)

#     # Create all tables
#     with connection.schema_editor() as schema_editor:
#         for db_table_name, models_group in models_by_table.items():
#             # Only create tables if migration is allowed
#             # - router allows it (not some external database)
#             # - model is managed (not by default)
#             # - user overrides this (e.g. developer)
#             # - create table for latest version of this dataset group
#             model = max(models_group, key=lambda model: model._dataset.version)

#             router_allows = router.allow_migrate_model(model._meta.app_label, model)
#             if not router_allows:
#                 command.stdout.write(f"  Skipping externally managed table: {db_table_name}")
#                 continue

#             if not allow_unmanaged and not model._meta.can_migrate(connection):
#                 command.stderr.write(f"  Skipping non-managed model: {model._meta.db_table}")
#                 continue

#             try:
#                 command.stdout.write(f"* Creating table {model._meta.db_table}")
#                 with transaction.atomic():
#                     schema_editor.create_model(model)
#             except (DatabaseError, ValueError) as e:
#                 command.stderr.write(f"  Tables not created: {e}")
#                 if not re.search(r'relation "[^"]+" already exists', str(e)):
#                     errors += 1

#     if errors:
#         raise CommandError("Not all tables could be created")
