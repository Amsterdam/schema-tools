from __future__ import annotations

import subprocess
import tempfile
from collections import deque
from pathlib import Path

from django.conf import settings
from django.core.management import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS

from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.management.commands.migration_helpers import migrate
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
            help=(
                "Get the tables from the filesystem. "
                "Note the SCHEMA_URL also needs to be file-based!"
            ),
        )
        parser.add_argument("schema", help="Schema name")
        parser.add_argument("table", help="Table name")
        # Currently, the old and new version needs to be given.
        # There is no way yet to retrieve a listing of available table versions
        parser.add_argument(
            "version1",
            metavar="OLDVERSION",
            help=(
                "Old table version, e.g. v1.0.0, or a git ref like"
                " `master`, `tag`, `branch` or `hash` with --from-files"
            ),
        )
        parser.add_argument(
            "version2",
            metavar="NEWVERSION",
            help=(
                "New table version, e.g. v1.1.0, , or a git ref like"
                " `master`, `tag`, `branch` or `hash` with --from-files"
            ),
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
            if options["schema_url"].startswith("http"):
                raise CommandError(
                    "The --from-files can only work with a SCHEMA_URL on the local filesystem."
                )

            with tempfile.TemporaryDirectory() as tmpdir:
                schemas_root = Path(options["schema_url"]).parent
                subprocess.run(  # noqa: S603
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

        migrate(
            self,
            dummy_dataset,
            dummy_dataset,  # In this case we can reuse the dummy dataset
            table1,
            table2,
            real_apps=real_apps,
            dry_run=True,  # We do not apply the migrations here, just show the SQL.
            database=options["database"],
        )

    def _load_table_from_checkout(
        self, dataset_id: str, table_id: str, tmpdir: str, version_ref: str
    ) -> DatasetTableSchema:
        """Load a DatasetTableSchema for the specified git reference."""
        subprocess.run(  # noqa: S603
            ["git", "checkout", version_ref], cwd=tmpdir, stdout=subprocess.DEVNULL
        )
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
                available = "', '".join(dataset.table_ids)
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
        # And when a call to DjangoModelFactory.build_model() triggers loading of more schemas,
        # these are also picked up from the deque() collection object.
        while self.schema_dependencies:
            dataset_schema = self.schema_dependencies.popleft()
            if dataset_schema.id == dataset.id:
                continue

            if self.verbosity >= 2:
                self.stdout.write(f"-- Building models for {dataset_schema.id}")
            DjangoModelFactory(self._get_dummy_dataset_model(dataset_schema)).build_models()
            real_apps.append(dataset_schema.id)

        return real_apps

    def _has_versioned_table(self, dataset: DatasetSchema, table_id: str) -> bool:
        """Tell whether the dataset has a versioned table with the given ID."""
        # Normally get_table_by_id() can be used, but for this migration command
        # that means unnecessary loading of unrelated tables. Yet the same snake-case logic
        # needs to be applied that get_table_by_id() also does.
        snaked_table_id = to_snake_case(table_id)
        return any(to_snake_case(table_id) == snaked_table_id for table_id in dataset.table_ids)

    def _get_dummy_dataset_model(self, dataset_schema: DatasetSchema) -> Dataset:
        """Generate a dummy "Dataset" object because DjangoModelFactory.build_model()
        needs this.
        """
        dataset = Dataset(
            name=dataset_schema.id, schema_data=dataset_schema.json(inline_tables=True)
        )
        # Hack the same logic that Dataset.create_for_schema() does
        # without actually creating a database object.
        dataset._loader = dataset_schema.loader
        dataset.__dict__["schema"] = dataset_schema
        return dataset
