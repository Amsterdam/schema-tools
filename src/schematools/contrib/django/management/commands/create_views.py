from __future__ import annotations

from collections.abc import Iterable

from django.core.management import BaseCommand, CommandError
from django.db import DatabaseError, connection, transaction
from psycopg import sql

from schematools.contrib.django.models import Dataset, DatasetTableSchema
from schematools.naming import to_snake_case

DATASETS = Dataset.objects.db_enabled()


class Command(BaseCommand):
    help = "Create the views based on the uploaded Amsterdam schema's."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):
        create_views(self, Dataset.objects.db_enabled())


def _is_valid_sql(view_sql: str, view_name: str, write_role_name: str) -> bool:
    """Try and execute the SQL return true if it succeeds, rollback the transaction."""
    try:
        with transaction.atomic():
            sid = transaction.savepoint()
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = 5000;")
                cursor.execute(sql.SQL(view_sql))
            transaction.savepoint_rollback(sid)
    except Exception as e:  # noqa: F841, BLE001
        return False
    return True


def _get_scopes(datasetname: str, tablename: str) -> frozenset[str]:
    from functools import reduce
    from operator import __or__

    dataset = DATASETS.get(name=to_snake_case(datasetname)).schema
    if tablename in [table.id for table in dataset.tables]:
        table = dataset.get_table_by_id(tablename)
        return dataset.auth | table.auth | reduce(__or__, [f.auth for f in table.fields])
    return frozenset()


def _get_required_permissions(
    table: DatasetTableSchema,
) -> frozenset[str]:
    derived_from = table.derived_from
    all_scopes = frozenset()
    for relation in derived_from:
        datasetname, tablename = relation.split(":")
        all_scopes |= _get_scopes(datasetname, tablename)
    if len(all_scopes) > 1:
        all_scopes = frozenset(set(all_scopes) - {"OPENBAAR"})
    return all_scopes


def _check_required_permissions_exist(
    view_dataset_auth: frozenset[str], required_permissions: frozenset[str]
) -> bool:
    """Check if the required permissions exist in the table and dataset auth.

    Args:
        view_dataset_auth (frozenset[str]): The auth parameter of the dataset the view is in.
        required_permissions (frozenset[str]): The required permissions for the view.

    returns:
        bool: True if all required permissions exist in the view dataset auth, False if not.

    """

    return required_permissions <= (view_dataset_auth | {"OPENBAAR"})


def _clean_sql(sql) -> str:
    """Clean the SQL to make it easier to parse."""

    return sql.replace("\n", " ").strip()


def _create_role_if_not_exists(cursor, role_name):
    # Create the role if it doesn't exist
    cursor.execute(
        sql.SQL("SELECT 1 FROM pg_roles WHERE rolname={role_name}").format(
            role_name=sql.Literal(role_name)
        )
    )

    role_exists = cursor.fetchone()
    if not role_exists:
        cursor.execute(
            sql.SQL("CREATE ROLE {role_name}").format(role_name=sql.Identifier(role_name))
        )


def create_views(
    command: BaseCommand,
    datasets: Iterable[Dataset],
    base_app_name: str | None = None,
    dry_run: bool = False,
) -> None:
    """Create views. This is a separate function to allow easy reuse."""
    errors = 0
    command.stdout.write("Creating views")

    # Because datasets are related, we need to 'prewarm'
    # the datasets cache (encapsulated in the DatasetSchema.loader)
    # by accessing the `Dataset.schema` attribute.
    for dataset in datasets:
        dataset.schema  # noqa: B018

    for dataset in datasets:
        if not dataset.enable_db:
            continue  # in case create_views() is called by import_schemas

    # Create all views
    for dataset in datasets:
        for table in dataset.schema.tables:
            if table.is_view:
                command.stdout.write(f"* Creating view {table.db_name}")
                # Generate the write role name
                write_role_name = f"write_{table._parent_schema.db_name}"

                # Check if the view sql is valid
                # If not skip this view and proceed with next view
                view_sql = _clean_sql(dataset.schema.get_view_sql())
                view_type = "materialized" if "materialized" in view_sql.lower() else "view"
                if not _is_valid_sql(view_sql, table.db_name, write_role_name):
                    command.stderr.write(f"  Invalid SQL for view {table.db_name}")
                    continue

                required_permissions = _get_required_permissions(table)
                view_dataset_auth = (
                    dataset.schema.auth
                    if view_type == "view"
                    else _get_scopes(dataset.name, table.id)
                )

                if _check_required_permissions_exist(view_dataset_auth, required_permissions):
                    if dry_run:
                        command.stdout.write("  The following sql would be executed:")
                        command.stdout.write(f"  {view_sql}")
                        continue
                    try:
                        with connection.cursor() as cursor:

                            # Check if write role exists and create if it does not
                            _create_role_if_not_exists(cursor, write_role_name)

                            # Grant usage and create on schema public to write role
                            cursor.execute(
                                sql.SQL(
                                    "GRANT usage,create on schema public TO {write_role_name}"
                                ).format(write_role_name=sql.Identifier(write_role_name))
                            )

                            # We create one `view_owner` role that owns all views
                            _create_role_if_not_exists(cursor, "view_owner")

                            cursor.execute("GRANT view_owner TO current_user")

                            cursor.execute(
                                sql.SQL("GRANT {write_role_name} TO view_owner").format(
                                    write_role_name=sql.Identifier(write_role_name)
                                )
                            )

                            # Loop though all required permissions and and grant them to the
                            # write user
                            for scope in required_permissions:
                                scope = f'scope_{scope.replace("/", "_").lower()}'
                                if scope:
                                    cursor.execute(
                                        sql.SQL("GRANT {scope} TO {write_role_name}").format(
                                            scope=sql.Identifier(scope),
                                            write_role_name=sql.Identifier(write_role_name),
                                        )
                                    )

                            cursor.execute(
                                sql.SQL("GRANT scope_openbaar TO {write_role_name}").format(
                                    write_role_name=sql.Identifier(write_role_name)
                                )
                            )

                            # Set the role before creating the view because the view is created
                            # with the permissions of the role
                            cursor.execute(
                                sql.SQL("SET ROLE {write_role_name}").format(
                                    write_role_name=sql.Identifier(write_role_name)
                                )
                            )

                            # Remove the view if it exists
                            # Due to the large costs of recreating materialized views,
                            # we only create and not drop them. When changes are made
                            # to the materialized view the view must be dropped manually.
                            if view_type != "materialized":
                                cursor.execute(
                                    sql.SQL("DROP VIEW IF EXISTS {view_name} CASCADE").format(
                                        view_name=sql.Identifier(table.db_name)
                                    )
                                )

                            # Create the view
                            cursor.execute(sql.SQL(view_sql))

                            # Reset the role to the default role
                            cursor.execute("RESET ROLE")

                            # Remove create and usage from write role
                            cursor.execute(
                                sql.SQL(
                                    "REVOKE usage,create on schema public FROM {write_role_name}"
                                ).format(write_role_name=sql.Identifier(write_role_name))
                            )

                            cursor.close()
                    except (DatabaseError, ValueError) as e:
                        command.stderr.write(f"  View not created: {e}")
                        errors += 1
                else:
                    command.stderr.write(
                        f"  Required permissions for view {table.db_name}"
                        " are not in the view dataset auth"
                    )

    if errors:
        raise CommandError("Not all views could be created")
