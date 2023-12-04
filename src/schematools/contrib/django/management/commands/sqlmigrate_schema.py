from __future__ import annotations

import os
import subprocess
import tempfile
from collections import deque
from pathlib import Path

from django.apps import apps
from django.conf import settings
from django.core.management import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.migrations import Migration
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.questioner import InteractiveMigrationQuestioner
from django.db.migrations.state import ModelState, ProjectState

from schematools.contrib.django.factories import model_factory, schema_models_factory
from schematools.contrib.django.models import Dataset
from schematools.exceptions import DatasetNotFound, DatasetTableNotFound
from schematools.loaders import SchemaLoader, get_schema_loader
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema, DatasetTableSchema


class Command(BaseCommand):
    """Management command to generate SQL statements to migrate Amsterdam Schema changes.
    Example usage::

        ./manage.py sqlmigrate_schema -v3 meetbouten meetbouten v1.0.0 v1.1.0

        or, using the schemas from local filesystem and getting the
        older version of a schema from a git reference (can be a branch/tag/hash):

        ./manage.py sqlmigrate_schema -v3 meetbouten meetbouten \
                7d986c96 \
                master \
                ---from-files
    The command is sped up by pointing ``SCHEMA_URL`` or ``--schema-url``
    to a local filesystem repository of the schema files. Otherwise it downloads
    the current schemas from the default remote repository.
    """

    requires_system_checks = []
    help = """Print the SQL statements to migrate between two schema versions."""

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help='Nominates a database to create SQL for. Defaults to the "default" database.',
        )
        parser.add_argument(
            "--from-files",
            action="store_true",
            help="Get the tables from the filesystem. NB. the SCHEMA_URL also needs to be file-based!",
        )
        parser.add_argument("schema", help="Schema name")
        parser.add_argument("table", help="Table name")
        # Currently, the old and new version needs to be given.
        # There is no way yet to retrieve a listing of available table versions
        parser.add_argument(
            "version1",
            metavar="OLDVERSION",
            help="Old table version, e.g. v1.0.0, or a git ref like `master`, `tag`, `branch` or `hash` with --from-files",
        )
        parser.add_argument(
            "version2",
            metavar="NEWVERSION",
            help="New table version, e.g. v1.1.0, , or a git ref like `master`, `tag`, `branch` or `hash` with --from-files",
        )

    def handle(self, *args, **options) -> None:
        # Retrieve schema objects, and bail out with proper errors.
        self.schema_dependencies = deque()
        self.verbosity = options["verbosity"]
        self.loader = get_schema_loader(
            options["schema_url"], loaded_callback=self._loaded_callback
        )

        # Load the data from the schema repository
        dataset = self._load_dataset(options["schema"])

        # For the from_files option, we check out the schemas repo
        # in a temporary directory.
        # By checking out 2 different git references, we can
        # obtain the tables for these specific references
        # for comparison and sql generation.
        if options["from_files"]:
            assert not options["schema_url"].startswith(
                "http"
            ), "The --from-files can only work with a SCHEMA_URL on the local filesystem."
            with tempfile.TemporaryDirectory() as tmpdir:
                schemas_root = Path(options["schema_url"]).parent
                subprocess.run(  # nosec
                    ["git", "clone", schemas_root, tmpdir],
                )
                table1 = self._load_table_from_checkout(
                    dataset.id, options["table"], tmpdir, options["version1"]
                )
                table2 = self._load_table_from_checkout(
                    dataset.id, options["table"], tmpdir, options["version2"]
                )
        else:
            table1 = self._load_table_version(dataset, options["table"], options["version1"])
            table2 = self._load_table_version(dataset, options["table"], options["version2"])
        real_apps = self._load_dependencies(dataset)
        dummy_dataset = self._get_dummy_dataset_model(dataset)

        # Generate a full project state that incorporates the table versions
        # The used Django migration-API calls were inspired by reading
        # the 'manage.py makemigrations' and 'manage.py sqlmigrate' command changes.
        base_state = self._get_base_project_state(dummy_dataset, table1.id, real_apps=real_apps)
        state1 = self._get_versioned_project_state(base_state, dummy_dataset, table1)
        state2 = self._get_versioned_project_state(base_state, dummy_dataset, table2)

        # Clear any models from the app cache to avoid confusion
        del apps.all_models[dataset.id]
        del apps.app_configs[dataset.id]
        apps.clear_cache()

        # Let the migration engine perform its magic, similar to `manage.py makemigrations`:
        migrations = self._get_migrations(state1, state2, app_name=dataset.id)
        if not migrations:
            self.stdout.write("No changes detected")
            return

        # Generate SQL per migration file, just like `manage.py sqlmigrate` does:
        start_state = state1
        connection = connections[options["database"]]
        for app, app_migrations in migrations.items():
            for migration in app_migrations:
                start_state = self._print_sql(connection, start_state, migration)

    def _load_table_from_checkout(
        self, dataset_id: str, table_id: str, tmpdir: str, version_ref: str
    ) -> DatasetTableSchema:
        """Load a DatasetTableSchema for the specified git reference."""
        subprocess.run(["git", "checkout", version_ref], cwd=tmpdir, stdout=subprocess.DEVNULL)
        tmp_schema_path = Path(tmpdir) / "datasets"
        # We create a specific schema loader, because it has to read in the data
        # associated with a specific git checkout.
        loader = get_schema_loader(str(tmp_schema_path), loaded_callback=self._loaded_callback)
        return self._load_table_version_from_file(loader, dataset_id, table_id)

    def _loaded_callback(self, schema: DatasetSchema):
        """Track which schema's get loaded. This is also used for dependency tracking."""
        if self.verbosity >= 1:
            self.stdout.write(f"-- Loading dataset {schema.id}")

        self.schema_dependencies.append(schema)

    def _load_dataset(self, dataset_id: str) -> DatasetSchema:
        """Load a dataset, bail out with a proper CLI message."""
        try:
            return self.loader.get_dataset(dataset_id, prefetch_related=True)
        except DatasetNotFound as e:
            raise CommandError(str(e)) from e

    def _load_table_version(
        self, dataset: DatasetSchema, table_id: str, version: str
    ) -> DatasetTableSchema:
        """A separate method to retrieve the table, so better error messages can be shown."""
        try:
            return self.loader.get_table(dataset, f"{table_id}/{version}")
        except DatasetTableNotFound as e:
            if not self._has_versioned_table(dataset, table_id):
                # Better error message if the table doesn't exist at all.
                # No need to use get_table_by_id() as that also loads other tables.
                available = "', '".join(dataset.table_versions.keys())
                raise CommandError(
                    f"Dataset '{dataset.id}' has no versioned table named '{table_id}', "
                    f"available are: '{available}'"
                ) from None

            raise CommandError(f"Table version '{table_id}/{version}' does not exist.") from e

    def _load_table_version_from_file(
        self, loader: SchemaLoader, dataset_id: str, table_id: str
    ) -> DatasetTableSchema:
        dataset = loader.get_dataset(dataset_id, prefetch_related=True)
        return dataset.get_table_by_id(table_id)

    def _load_dependencies(self, dataset: DatasetSchema) -> list[str]:
        """Make sure any dependencies are loaded.

        Returns the list of "real app names", which tells Django migrations those apps
        are not part of the project state, but can be found in the main app registry itself.
        """
        related_ids = dataset.related_dataset_schema_ids
        real_apps = []

        # Load first, and this fills the cache.
        for dataset_id in related_ids - {dataset.id}:
            self.loader.get_dataset(dataset_id, prefetch_related=True)

        # Turn any loaded schema into a model.
        # And when a call to model_factory() triggers loading of more schemas,
        # these are also picked up from the deque() collection object.
        while self.schema_dependencies:
            dataset_schema = self.schema_dependencies.popleft()
            if dataset_schema.id == dataset.id:
                continue

            if self.verbosity >= 2:
                self.stdout.write(f"-- Building models for {dataset_schema.id}")
            schema_models_factory(self._get_dummy_dataset_model(dataset_schema))
            real_apps.append(dataset_schema.id)

        return real_apps

    def _has_versioned_table(self, dataset: DatasetSchema, table_id: str) -> bool:
        """Tell whether the dataset has a versioned table with the given ID."""
        # Normally get_table_by_id() can be used, but for this migration command
        # that means unnecessary loading of unrelated tables. Yet the same snake-case logic
        # needs to be applied that get_table_by_id() also does.
        snaked_table_id = to_snake_case(table_id)
        return any(
            to_snake_case(table_id) == snaked_table_id
            for table_id in dataset.table_versions.keys()
        )

    def _get_dummy_dataset_model(self, dataset_schema: DatasetSchema) -> Dataset:
        """Generate a dummy "Dataset" object because model_factory() needs this."""
        dataset = Dataset(
            name=dataset_schema.id, schema_data=dataset_schema.json(inline_tables=True)
        )
        # Hack the same logic that Dataset.create_for_schema() does
        # without actually creating a database object.
        dataset._dataset_collection = dataset_schema.loader
        dataset.__dict__["schema"] = dataset_schema
        return dataset

    def _get_base_project_state(
        self, dataset_model: Dataset, exclude_table: str, real_apps: list[str]
    ) -> ProjectState:
        """Generate the common/shared project state.

        This includes all other models of the dataset, as those may be referenced by relations.
        It excludes the versioned table, since that will differ.
        """
        if self.verbosity >= 2:
            self.stdout.write(f"-- Building shared state for {dataset_model.name}")

        project_state = ProjectState(real_apps=set(real_apps))

        # Generate model states for all other tables in the dataset
        for table in dataset_model.schema.get_tables(include_nested=True, include_through=True):
            # Exclude the actual table that changes, including any nested/through tables.
            if table.id == exclude_table or (
                table.has_parent_table and table.parent_table.id == exclude_table
            ):
                continue

            project_state.add_model(self._get_model_state(dataset_model, table))

        return project_state

    def _get_versioned_project_state(
        self, base_state: ProjectState, dataset_model: Dataset, table: DatasetTableSchema
    ) -> ProjectState:
        """Generate the final project state.
        This clones the base state, so other related models are only created once.
        """
        project_state = base_state.clone()
        project_state.add_model(self._get_model_state(dataset_model, table))

        # Add any nested tables and through tables,
        # that are also part of this table schema.
        for field in table.fields:
            if (through_table := field.through_table) is not None:
                project_state.add_model(self._get_model_state(dataset_model, through_table))
            elif (nested_table := field.nested_table) is not None:
                project_state.add_model(self._get_model_state(dataset_model, nested_table))

        return project_state

    def _get_model_state(
        self, dataset_model: Dataset, table: DatasetTableSchema, managed=True
    ) -> ModelState:
        """Generate the model state for a table."""
        # The migration-engine will only consider models that have "managed=True".
        # This is turned off by default for the model_factory() logic,
        # and needs to be overwritten here (patching model._meta and its original_attrs is nasty).
        model_class = model_factory(dataset_model, table, meta_options={"managed": managed})

        # Generate the model. exclude_rels=True because M2M-through tables are generated manually.
        return PatchedModelState.from_model(model_class, exclude_rels=True)

    def _get_migrations(
        self, state1: ProjectState, state2: ProjectState, app_name: str
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
            migration_name="dummy",
        )

    def _print_sql(
        self, connection: BaseDatabaseWrapper, start_state: ProjectState, migration: Migration
    ) -> ProjectState:
        """Print the SQL statements for a migration"""
        if self.verbosity >= 3:
            self.stdout.write(f"-- Migration: {migration.name}")
            for operation in migration.operations:
                self.stdout.write(f"--   {operation}")

        with connection.schema_editor(collect_sql=True, atomic=migration.atomic) as schema_editor:
            try:
                start_state = migration.apply(start_state, schema_editor, collect_sql=True)
            except Exception:
                # On crashes, still show the generated statements so far
                self.stdout.write("\n".join(schema_editor.collected_sql))
                raise

        self.stdout.write("\n".join(schema_editor.collected_sql))
        return start_state


class PatchedModelState(ModelState):
    """A workaround to avoid breaking migration rendering."""

    def clone(self):
        """Return an exact copy of this ModelState."""
        # Workaround for Django issue. The fields get bound to a model during the first
        # migration operation, which breaks reusing them in migrations.
        # Quick fix is to deep-clone the fields too:
        self.fields = {name: field.clone() for name, field in self.fields.items()}
        return super().clone()
