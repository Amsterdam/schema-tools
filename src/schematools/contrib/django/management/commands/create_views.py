import re
from collections import defaultdict, abc
from typing import Iterable, List, Optional
import requests
import sqlvalidator

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset, DatasetTableSchema

DATASETS = Dataset.objects.db_enabled()


class Command(BaseCommand):
    help = "Create the views based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):
        create_views(self, Dataset.objects.db_enabled())


def _get_scopes(datasetname: str, tablename: str) -> str:
    all_scopes = []
    tables = DATASETS.get(name=datasetname).schema.tables
    for table in tables:
        if table.id == tablename:
            # Found table name, now extract scopes from main auth and field auths
            if table.auth is not None:
                if not isinstance(table.auth, abc.Iterable):
                    if table.auth not in all_scopes:
                        all_scopes.append(list(table.auth)[0])
                else:
                    for scope in table.auth:
                        if scope not in all_scopes:
                            all_scopes.append(scope)
            else:
                schema_auth = list(DATASETS.get(name=datasetname).schema.auth)[0]
                if schema_auth:
                    if not isinstance(schema_auth, abc.Iterable):
                        if schema_auth not in all_scopes:
                            all_scopes.append(schema_auth)
                    else:
                        for scope in schema_auth:
                            if scope not in all_scopes:
                                all_scopes.append(scope)
            for field in table.fields:
                if field.auth is not None:
                    if not isinstance(field.auth, abc.Iterable):
                        if field.auth not in all_scopes:
                            all_scopes.append(list(field.auth)[0])
                    else:
                        for scope in field.auth:
                            if scope not in all_scopes:
                                all_scopes.append(scope)
    return all_scopes


def _get_required_permissions(
    table: DatasetTableSchema,
) -> list[str]:
    derived_from = table.derived_from
    all_scopes = []
    for relation in derived_from:
        datasetname, tablename = relation.split(":")
        scopes = _get_scopes(datasetname, tablename)
        for scope in scopes:
            if scope not in all_scopes:
                all_scopes.append(scope)
    return all_scopes

def _check_permissions_exist(table_auth: frozenset, dataset_auth: frozenset, required_permissions: list[str]) -> bool:
    for permission in required_permissions:
        import pdb; pdb.set_trace()
        if permission not in table_auth or permission not in dataset_auth:
            return False
    return True

def _is_valid_sql(sql: str) -> bool:
    sql_query = sqlvalidator.parse(sql)
    if not sql_query.is_valid():
        return False
    else:
        return True

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
            continue  # in case create_views() is called by import_schemas

    # Create all views
    for dataset in datasets:
        for table in dataset.schema.tables:
            if table.is_view:
                import pdb; pdb.set_trace()
                view_sql = dataset.schema.get_view_sql()
                required_permissions = _get_required_permissions(table)
                if _check_permissions_exist(table.auth, dataset.schema.auth, required_permissions):
                    try:
                        with connection.cursor() as cursor:
                            # Loop though all required permissions and set the role
                            for scope in required_permissions:
                                scope = f'scope_{scope.replace("/", "_").lower()}'
                                cursor.execute(
                                    f"GRANT {scope} TO write_{table._parent_schema.db_name}"
                                )
                            cursor.execute(f"SET ROLE write_{table._parent_schema.db_name}")
                            if not _is_valid_sql(view_sql):
                                raise CommandError(f"Invalid view SQL for {table.id}")
                            cursor.execute(view_sql)
                            command.stdout.write(f"* Creating view {table.id}")
                    except (DatabaseError, ValueError) as e:
                        command.stderr.write(f"  Views not created: {e}")
                        errors += 1
                else:
                    raise CommandError(f"Required permissions for view {table.id} do not exist")

    if errors:
        raise CommandError("Not all views could be created")
