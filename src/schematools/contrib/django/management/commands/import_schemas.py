from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand
from django.db.models import Q

from schematools._datasetcollection import _DatasetCollection, _set_schema_loader
from schematools.contrib.django.models import Dataset
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema
from schematools.utils import dataset_schema_from_path

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
        for filename in schema_files:
            self.stdout.write(f"Loading schema from {filename}")
            schema = dataset_schema_from_path(filename)
            dataset = self._import(schema, filename)
            datasets.append(dataset)

        return datasets

    def import_from_url(self, schema_url) -> List[Dataset]:
        """Import all schema definitions from a URL"""
        self.stdout.write(f"Loading schema from {schema_url}")
        _set_schema_loader(schema_url)
        dataset_collection = _DatasetCollection()
        datasets = []

        schemas = dataset_collection.get_all_datasets()
        for path, schema in schemas.items():
            self.stdout.write(f"* Processing {schema.id}")
            dataset = self._import(schema, path)
            if dataset is not None:
                datasets.append(dataset)

        return datasets

    def _import(self, schema: DatasetSchema, path: str) -> Optional[Dataset]:
        """Import a single dataset schema."""
        created = False
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
                created = True

        if created:
            self.stdout.write(f"  Created {schema.id}")
            return dataset
        else:
            self.stdout.write(f"  Updated {schema.id}")

            if dataset.is_default_version != schema.is_default_version:
                self.update_dataset_version(dataset, schema)

            updated = dataset.save_for_schema(schema)
            dataset.save_path(path)
            if updated:
                return dataset

        return None

    def update_dataset_version(self, dataset: Dataset, schema: DatasetSchema) -> Dataset:
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
