from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand
from django.db.models import Q

from schematools.contrib.django.models import Dataset
from schematools.loaders import FileSystemSchemaLoader, get_schema_loader
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema

from .create_tables import create_tables


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
        parser.add_argument("--no-create-tables", dest="create_tables", action="store_false")
        parser.set_defaults(create_tables=False)

    def handle(self, *args, **options):
        if options["schema"]:
            datasets = self.import_from_files(options["schema"])
        else:
            datasets = self.import_from_url(options["schema_url"])

        if not datasets:
            self.stdout.write("No new datasets imported")
            return

        # Reasons for not creating tables directly are to manually configure the
        # "Datasets" model flags first. E.g. disable "enable_db", set a remote URL.
        if options["create_tables"]:
            create_tables(self, datasets, allow_unmanaged=True)

    def import_from_files(self, schema_files) -> List[Dataset]:
        """Import all schema definitions from the given files."""
        datasets = []
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
                files = _get_shared_loader(file).get_all_datasets()
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

                files = {path: dataset}

            datasets.extend(self._run_import(files))

        return datasets

    def import_from_url(self, schema_url) -> List[Dataset]:
        """Import all schema definitions from a URL"""
        self.stdout.write(f"Loading schema from {schema_url}")
        loader = get_schema_loader(schema_url, loaded_callback=self._loaded_callback)
        return self._run_import(loader.get_all_datasets())

    def _loaded_callback(self, schema: DatasetSchema):
        self.stdout.write(f"* Loaded {schema.id}")

    def _run_import(self, files: dict[str, DatasetSchema]) -> list[Dataset]:
        datasets = []
        for path, schema in files.items():
            self.stdout.write(f"* Processing {schema.id}")
            dataset = self._import(schema, path)
            if dataset is not None:
                datasets.append(dataset)

        return datasets

    def _import(self, schema: DatasetSchema, path: str) -> Optional[Dataset]:
        """Import a single dataset schema."""
        try:
            dataset = Dataset.objects.get(name=Dataset.name_from_schema(schema))
        except Dataset.DoesNotExist:
            try:
                # try getting default dataset by name and version
                dataset = Dataset.objects.filter(Q(version=None) | Q(version=schema.version)).get(
                    name=to_snake_case(schema.id)
                )
            except Dataset.DoesNotExist:
                # Give up, Create new dataset
                dataset = Dataset.create_for_schema(schema, path)
                self.stdout.write(f"  Created {schema.id}")
                return dataset

        self.stdout.write(f"  Updated {schema.id}")
        if dataset.is_default_version != schema.is_default_version:
            self.update_dataset_version(dataset, schema)

        updated = dataset.save_for_schema(schema)
        dataset.save_path(path)
        return dataset if updated else None

    def update_dataset_version(self, dataset: Dataset, schema: DatasetSchema) -> None:
        """
        Perform dataset version update, including changes to dataset tables.
        """
        if not dataset.is_default_version:
            # Dataset is currently not default. Can not be safely renamed.
            if schema.is_default_version:
                # Dataset is promoted to default. We need to rename current default,
                #  if it was not done yet.
                try:
                    current_default = Dataset.objects.get(name=Dataset.name_from_schema(schema))
                except Dataset.DoesNotExist:
                    pass
                else:
                    # Update current default dataset name to expected name.
                    if current_default.version:
                        current_default.name = to_snake_case(
                            f"{schema.id}_{current_default.version}"
                        )

                        current_default.save()

        dataset.name = Dataset.name_from_schema(schema)
        dataset.is_default_version = schema.is_default_version
