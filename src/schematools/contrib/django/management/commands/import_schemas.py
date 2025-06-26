from __future__ import annotations

from collections import deque
from pathlib import Path

from django.conf import settings
from django.core.management import BaseCommand
from django.db import transaction

from schematools import validation
from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.management.commands.migration_helpers import drop_table, migrate
from schematools.contrib.django.models import Dataset
from schematools.loaders import FileSystemSchemaLoader, get_schema_loader
from schematools.types import DatasetSchema, DatasetTableSchema

from .create_tables import create_tables
from .create_views import create_views


class Command(BaseCommand):
    help = """Import Amsterdam schema files.

    import_schemas imports all schemas from $SCHEMA_URL,
    unless schema files/URLs are given as positional arguments.
    """
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("schema", nargs="*", help="Local schema files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument("--create-tables", dest="create_tables", action="store_true")
        parser.add_argument("--create-views", dest="create_views", action="store_true")
        parser.add_argument("--no-create-tables", dest="create_tables", action="store_false")
        parser.add_argument("--dry-run", dest="dry_run", action="store_true")
        parser.add_argument("--execute", dest="dry_run", action="store_false")
        parser.set_defaults(create_tables=False)
        parser.set_defaults(create_views=False)
        parser.set_defaults(dry_run=True)

    def handle(self, *args, **options):
        self.dry_run = options["dry_run"]
        self.verbosity = options["verbosity"]
        self.schema_dependencies = deque()
        current_datasets = {
            Dataset.name_from_schema(ds.schema): ds for ds in Dataset.objects.all()
        }

        if options["schema"]:
            schemas = self.get_schemas_from_files(options["schema"])
        else:
            schemas = self.get_schemas_from_url(options["schema_url"])

        with transaction.atomic():
            # Contains unsaved Dataset objects if dry_run.
            updated_datasets = self._run_import(schemas)
            if not updated_datasets:
                self.stdout.write("No new datasets imported")
                return

            # Loop over updated datasets and perform migrations.
            for updated_dataset in updated_datasets:
                current_dataset = current_datasets.get(
                    Dataset.name_from_schema(updated_dataset.schema)
                )
                if not current_dataset:  # New dataset
                    continue

                real_apps = self._load_dependencies(updated_dataset.schema, updated_dataset)
                for current_table in current_dataset.schema.tables:
                    updated_table = updated_dataset.schema.get_table_by_id(
                        current_table.id, include_nested=False, include_through=False
                    )
                    if current_table.version.vmajor == updated_table.version.vmajor:
                        # If the table is experimental and there are breaking changes to the table,
                        # drop the table
                        if (
                            current_table.lifecycle_status
                            == DatasetTableSchema.LifecycleStatus.experimental
                        ):
                            previous_fields = current_table.json_data()["schema"]["properties"]
                            next_fields = updated_table.json_data()["schema"]["properties"]
                            table_errors = validation.validate_table(previous_fields, next_fields)
                            if len(table_errors) > 0:
                                if not options["create_tables"]:
                                    self.stdout.write(
                                        "Not dropping table, as create_tables is set to false."
                                    )
                                elif self.dry_run:
                                    self.stdout.write(
                                        f"Would drop and replace table {current_table.db_name}."
                                    )
                                else:
                                    # drop the table and rely on create_tables to create it again.
                                    for field in current_table.fields:
                                        if through_table := field.through_table:
                                            drop_table(through_table.db_name)
                                    drop_table(current_table.db_name)
                                # do not migrate in this case.
                                continue

                        # Migrate the table, no breaking changes
                        migrate(
                            self,
                            current_dataset,
                            updated_dataset,
                            current_table,
                            updated_table,
                            real_apps,
                            dry_run=self.dry_run,
                        )

            # Reasons for not creating tables directly are to manually configure the
            # "Datasets" model flags first. E.g. disable "enable_db".
            if options["create_tables"]:
                create_tables(self, updated_datasets, allow_unmanaged=True, dry_run=self.dry_run)

            if options["create_views"]:
                create_views(self, updated_datasets, dry_run=self.dry_run)

    def get_schemas_from_files(self, schema_files) -> dict[str, DatasetSchema]:
        """Import all schema definitions from the given files."""
        schemas = {}
        shared_loaders = {}

        def _get_shared_loader(path):
            # Make sure all datasets that share the same root also share the same loader,
            # so relations between the separate files can be resolved.
            if (loader := shared_loaders.get(path)) is None:
                shared_loaders[path] = loader = FileSystemSchemaLoader(
                    path, loaded_callback=self._loaded_callback
                )
            return loader

        for filename in schema_files:
            self.stdout.write(f"Loading schemas from {filename}")
            file = Path(filename).resolve()
            if file.is_dir():
                # As intended, read all datasets
                # The folder might be a sub path in the repository,
                # in which case only a few datasets will be imported.
                schemas.update(_get_shared_loader(file).get_all_datasets())
            else:
                # Previous logic also allowed selecting a single file by name.
                # This still needs to resolve the root to resolve relations,
                # and to make sure the 'path' is correctly calculated.
                root = FileSystemSchemaLoader.get_root(file)
                dataset = _get_shared_loader(root).get_dataset_from_file(file)
                # Random files may not follow the folder/dataset.json convention,
                # calculate the path here instead of using loader.get_dataset_path().
                path = file.parent.relative_to(root)
                if str(path) == ".":
                    path = dataset.id  # workaround for unit tests

                schemas.update({path: dataset})
        return schemas

    def get_schemas_from_url(self, schema_url) -> dict[str, DatasetSchema]:
        """Import all schema definitions from a URL"""
        self.stdout.write(f"Loading schema from {schema_url}")
        self.loader = get_schema_loader(schema_url, loaded_callback=self._loaded_callback)
        return self.loader.get_all_datasets()

    def _loaded_callback(self, schema: DatasetSchema):
        """Track which schema's get loaded. This is also used for dependency tracking."""
        self.stdout.write(f"* Loaded {schema.id}")

        self.schema_dependencies.append(schema)

    def _load_dependencies(self, dataset_schema: DatasetSchema, dataset) -> list[str]:
        """Make sure any dependencies are loaded.

        Returns the list of "real app names", which tells Django migrations those apps
        are not part of the project state, but can be found in the main app registry itself.
        """
        related_ids = dataset_schema.related_dataset_schema_ids
        real_apps = []

        # Load first, and this fills the cache.
        for dataset_id in related_ids - {dataset_schema.id}:
            dataset_schema.loader.get_dataset(dataset_id, prefetch_related=True)

        # Turn any loaded schema into a model.
        # And when a call to DjangoModelFactory.build_model() triggers loading of more schemas,
        # these are also picked up from the deque() collection object.
        while self.schema_dependencies:
            dependency_schema = self.schema_dependencies.popleft()
            if dependency_schema.id == dataset_schema.id:
                continue

            if self.verbosity >= 2:
                self.stdout.write(f"-- Building models for {dependency_schema.id}")
            DjangoModelFactory(dataset).build_models()
            real_apps.extend(
                [f"{dependency_schema.id}_{vmajor}" for vmajor in dependency_schema.versions]
            )

        return real_apps

    def _run_import(self, dataset_schemas: dict[str, DatasetSchema]) -> list[Dataset]:
        datasets = []
        for id, schema in dataset_schemas.items():
            path = self.loader._get_dataset_path(id) if hasattr(self, "loader") else id
            self.stdout.write(f"* Processing {schema.id}")
            dataset = self._import(schema, path)
            if dataset is not None:
                datasets.append(dataset)

        return datasets

    def _import(self, schema: DatasetSchema, path: str) -> Dataset | None:
        """Import a single dataset schema."""
        try:
            dataset = Dataset.objects.get(name=Dataset.name_from_schema(schema))
            updated = dataset.save_for_schema(schema, path, save=not self.dry_run)
            if updated:
                self.stdout.write(f"  Updated {schema.id}")
                return dataset
        except Dataset.DoesNotExist:
            # Create new dataset
            dataset = Dataset.create_for_schema(schema, path, save=not self.dry_run)
            self.stdout.write(f"  Created {schema.id}")
            return dataset

        return None
