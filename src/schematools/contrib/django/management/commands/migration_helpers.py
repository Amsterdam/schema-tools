from __future__ import annotations

import re

from django.apps import apps
from django.core.management import BaseCommand
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.migrations import Migration
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.questioner import InteractiveMigrationQuestioner
from django.db.migrations.state import ModelState, ProjectState

from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.models import Dataset
from schematools.types import DatasetTableSchema


def migrate(
    command: BaseCommand,
    current_dataset: Dataset,
    updated_dataset: Dataset,
    current_table: DatasetTableSchema,
    updated_table: DatasetTableSchema,
    real_apps: list[str],
    dry_run: bool = True,
    database: str = DEFAULT_DB_ALIAS,
    project_state: ProjectState = None,
):
    """
    Creates a project state for both the current dataset and the updated dataset.
    """
    # Generate a full project state that incorporates the table versions
    # The used Django migration-API calls were inspired by reading
    # the 'manage.py makemigrations' and 'manage.py sqlmigrate' command changes.
    command.stdout.write(f"* Processing table {updated_table.id}")
    base_state = get_base_project_state(command, current_dataset, current_table.id, real_apps)
    current_state = project_state or get_versioned_project_state(
        base_state, current_dataset, current_table
    )
    updated_state = get_versioned_project_state(base_state, updated_dataset, updated_table)

    # Clear any models from the app cache to avoid confusion
    for vmajor in current_dataset.schema.versions:
        app_label = f"{current_dataset.schema.id}_{vmajor}"
        if app_label in apps.all_models:
            del apps.all_models[app_label]
        if app_label in apps.app_configs:
            del apps.app_configs[app_label]
    apps.clear_cache()

    start_state = current_state

    # Let the migration engine perform its magic, similar to `manage.py makemigrations`:
    migrations = get_migrations(
        current_state,
        updated_state,
        app_name=f"{current_dataset.schema.id}_{current_dataset.schema.default_version}",
    )
    if not migrations:
        command.stdout.write(f"  No changes detected for table {current_table.id}")
        return start_state

    # Generate SQL per migration file, just like `manage.py sqlmigrate` does:
    connection = connections[database]
    for _app, app_migrations in migrations.items():
        for migration in app_migrations:
            start_state = execute_migration(
                command, connection, start_state, migration, dry_run=dry_run
            )
    return start_state


def drop_table(table_id):
    """Drop a database table."""
    connection = connections[DEFAULT_DB_ALIAS]
    with connection.schema_editor() as schema_editor:
        schema_editor.execute(f"DROP TABLE IF EXISTS {table_id};")


def get_base_project_state(
    command: BaseCommand, dataset_model: Dataset, exclude_table: str, real_apps: list[str]
) -> ProjectState:
    """Generate the common/shared project state.

    This includes all other models of the dataset, as those may be referenced by relations.
    It excludes the versioned table, since that will differ.
    """
    if command.verbosity >= 2:
        command.stdout.write(f"-- Building shared state for {dataset_model.name}")

    project_state = ProjectState(real_apps=set(real_apps))

    # Generate model states for all other tables in the dataset
    for table in dataset_model.schema.get_tables(include_nested=True, include_through=True):
        # Exclude the actual table that changes, including any nested/through tables.
        if table.id == exclude_table or (
            table.has_parent_table and table.parent_table.id == exclude_table
        ):
            continue

        project_state.add_model(get_model_state(dataset_model, table))

    return project_state


def get_versioned_project_state(
    base_state: ProjectState, dataset_model: Dataset, table: DatasetTableSchema
) -> ProjectState:
    """Generate the final project state.
    This clones the base state, so other related models are only created once.
    """
    project_state = base_state.clone()
    project_state.add_model(get_model_state(dataset_model, table))

    # Add any nested tables and through tables,
    # that are also part of this table schema.
    for field in table.fields:
        if (through_table := field.through_table) is not None:
            project_state.add_model(get_model_state(dataset_model, through_table))
        elif (nested_table := field.nested_table) is not None:
            project_state.add_model(get_model_state(dataset_model, nested_table))

    return project_state


def get_model_state(dataset_model: Dataset, table: DatasetTableSchema, managed=True) -> ModelState:
    """Generate the model state for a table."""
    # The migration-engine will only consider models that have "managed=True".
    # This is turned off by default for the DjangoModelFactory.build_model() logic,
    # and needs to be overwritten here (patching model._meta and its original_attrs is nasty).
    factory = DjangoModelFactory(dataset_model)
    model_class = factory.build_model(table, meta_options={"managed": managed})

    # Generate the model. exclude_rels=True because M2M-through tables are generated manually.
    return PatchedModelState.from_model(model_class, exclude_rels=True)


def get_migrations(
    state1: ProjectState, state2: ProjectState, app_name: str
) -> dict[str, list[Migration]]:
    """Generate a migration object for the given table versions."""
    detector = MigrationAutodetector(
        from_state=state1,
        to_state=state2,
        questioner=InteractiveMigrationQuestioner(specified_apps=[app_name]),
    )
    # The dependency graph remains empty here, as we assume all other models are still
    # in place. We only compare the changes between 2 models of the same table.
    dependency_graph = MigrationGraph()
    return detector.changes(
        dependency_graph,
        trim_to_apps=[app_name],
        migration_name=app_name,
    )


def execute_migration(
    command: BaseCommand,
    connection: BaseDatabaseWrapper,
    start_state: ProjectState,
    migration: Migration,
    dry_run: bool = True,
) -> ProjectState:
    """Print the SQL statements for a migration"""
    with connection.schema_editor(collect_sql=True, atomic=migration.atomic) as schema_editor:
        command.stdout.write(f"-- Migration {migration.name} for dataset {migration.app_label}")
        try:
            start_state = migration.apply(start_state, schema_editor, collect_sql=True)
        except Exception:
            # On crashes, still show the generated statements so far
            command.stdout.write(
                "\n".join(_filter_alter_type_statements(schema_editor.collected_sql))
            )
            raise

        # Filter out ALTER COLUMN statements
        collected_sql = _filter_alter_type_statements(schema_editor.collected_sql)
        # Escape % signs
        collected_sql = _escape_comment_statements(collected_sql)

        # If we've only got comments left, skip this migration
        if all(statement.startswith("--") for statement in collected_sql):
            command.stdout.write("-- No actual SQL statements generated, skipping this migration")
            return start_state

        if dry_run:
            command.stdout.write("-- DRY RUN - the following would be executed:")
            command.stdout.write("\n".join(collected_sql))
        else:
            schema_editor.collect_sql = False
            schema_editor.execute("\n".join(collected_sql))
    return start_state


def _filter_alter_type_statements(sql: list) -> list:
    """Filter ALTER TYPE statements from the generated SQL since this causes issues
    with our views. This function can be removed once we either:
        - Drop our views before running the ALTER TYPE statements and recreate them
        - Patch the migration logic in Django to not create an ALTER TYPE statement when
          adding or changing a db_comment
    """
    return [s for s in sql if not re.search(r"ALTER COLUMN.+TYPE", s)]


def _escape_comment_statements(sql: list) -> list:
    """Escape percent signs in comments from the generated SQL since this causes issues
    when executing this on the database.
    """
    return [s.replace("%", "%%") if re.search(r"COMMENT.+", s) else s for s in sql]


class PatchedModelState(ModelState):
    """A workaround to avoid breaking migration rendering."""

    def clone(self):
        """Return an exact copy of this ModelState."""
        # Workaround for Django issue. The fields get bound to a model during the first
        # migration operation, which breaks reusing them in migrations.
        # Quick fix is to deep-clone the fields too:
        self.fields = {name: field.clone() for name, field in self.fields.items()}
        return super().clone()
