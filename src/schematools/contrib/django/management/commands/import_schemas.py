from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand
from django.db.models import Q

from schematools.contrib.django.models import Dataset
from schematools.types import DatasetSchema
from schematools.utils import schema_defs_from_url, to_snake_case

from .create_tables import create_tables


class Command(BaseCommand):
    help = """Import Amsterdam schema files.

    import_schemas imports all schemas from $SCHEMA_URL,
    unless schema files/URLs are given as positional arguments.
    """
    requires_system_checks = False

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
            schema = DatasetSchema.from_file(filename)
            dataset = self.import_schema(schema.id, schema)
            datasets.append(dataset)

        return datasets

    def import_from_url(self, schema_url) -> List[Dataset]:
        """Import all schema definitions from an URL"""
        self.stdout.write(f"Loading schema from {schema_url}")
        datasets = []

        for name, schema in schema_defs_from_url(schema_url).items():
            self.stdout.write(f"* Processing {name}")
            dataset = self.import_schema(name, schema)
            if dataset is not None:
                datasets.append(dataset)

        return datasets

    def import_schema(self, name: str, schema: DatasetSchema) -> Optional[Dataset]:
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
                dataset = Dataset.create_for_schema(schema)
                created = True

        if created:
            self.stdout.write(f"  Created {name}")
            return dataset
        else:
            self.stdout.write(f"  Updated {name}")

            if dataset.is_default_version != schema.is_default_version:
                self.update_dataset_version(dataset, schema)

            updated = dataset.save_for_schema(schema)
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
