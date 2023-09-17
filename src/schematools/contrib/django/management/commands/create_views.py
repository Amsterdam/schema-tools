import re
from collections import abc, defaultdict
from typing import Iterable, List, Optional

import requests
import sqlparse
from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, router, transaction

from schematools.contrib.django.factories import schema_models_factory
from schematools.contrib.django.models import Dataset, DatasetTableSchema
from schematools.naming import to_snake_case

DATASETS = Dataset.objects.db_enabled()


class Command(BaseCommand):
    help = "Create the views based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):
        create_views(self, Dataset.objects.db_enabled())


def _is_valid_sql(sql: str) -> bool:
    """Try and execute the SQL return true if it succeeds, rollback the transaction."""
    try:
        with transaction.atomic():
            sid = transaction.savepoint()
            with connection.cursor() as cursor:
                cursor.execute(sql)
            transaction.savepoint_rollback(sid)
    except Exception:
        return False
    return True


def _get_scopes(datasetname: str, tablename: str) -> list[str]:
    from operator import __or__
    from functools import reduce
    dataset = DATASETS.get(name=to_snake_case(datasetname)).schema
    if tablename in [table.id for table in dataset.tables]:
        table = dataset.get_table_by_id(tablename)
        return table.auth | reduce(__or__, [f.auth for f in table.fields])
    return []

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


def _check_required_permissions_exist(
    view_dataset_auth: frozenset, required_permissions: list[str]
) -> bool:
    """Check if the required permissions exist in the table and dataset auth.

    Args:
        view_dataset_auth (frozenset): The auth parameter of the dataset the view is in.
        required_permissions (list[str]): The required permissions for the view.

    returns:
        bool: True if all required permissions exist in the view dataset auth, False if not.

    """
    view_dataset_auth = list(map(str, view_dataset_auth))
    if len(required_permissions) > 1 and "OPENBAAR" in required_permissions:
        required_permissions.remove("OPENBAAR")
    for permission in required_permissions:
        if permission not in view_dataset_auth:
            return False
    return True


def _clean_sql(sql) -> str:
    """Clean the SQL to make it easier to parse."""

    sql = sql.replace("\n", " ").lower().strip()

    if sql[-1] != ";":
        sql += ";"

    return sql


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
                command.stdout.write(f"* Creating view {table.db_name}")
                view_sql = _clean_sql(dataset.schema.get_view_sql())
                if not _is_valid_sql(view_sql):
                    command.stderr.write(f"  Invalid SQL for view {table.db_name}")
                    errors += 1
                    continue
                required_permissions = _get_required_permissions(table)
                view_dataset_auth = dataset.schema.auth
                if _check_required_permissions_exist(view_dataset_auth, required_permissions):
                    try:
                        with connection.cursor() as cursor:
                            write_role_name = f"write_{table._parent_schema.db_name}"

                            # Remove the view if it exists
                            cursor.execute(f"DROP VIEW IF EXISTS {table.db_name} CASCADE")

                            # Create the role if it doesn't exist
                            cursor.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{write_role_name}'")
                            role_exists = cursor.fetchone()
                            if not role_exists:
                                cursor.execute(f"CREATE ROLE {write_role_name}")

                            # Loop though all required permissions and and grant them to the write user
                            for scope in required_permissions:
                                scope = f'scope_{scope.replace("/", "_").lower()}'
                                if scope:
                                    cursor.execute(f"GRANT {scope} TO {write_role_name}")
                            cursor.execute(f"GRANT scope_openbaar TO {write_role_name}")

                            # Set the role before creating the view because the view is created with the permissions of the role
                            cursor.execute(f"SET ROLE {write_role_name}")

                            # Create the view
                            cursor.execute(view_sql)

                            # Reset the role to the default role
                            cursor.execute("RESET ROLE")
                            cursor.close()
                    except (DatabaseError, ValueError) as e:
                        command.stderr.write(f"  View not created: {e}")
                        errors += 1
                else:
                    command.stderr.write(
                        f"  Required permissions {required_permissions} not found in view dataset auth"
                    )

    if errors:
        raise CommandError("Not all views could be created")
