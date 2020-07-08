from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand

from schematools.contrib.django.models import Dataset
from schematools.types import DatasetSchema
from schematools.utils import schema_defs_from_url, to_snake_case

from .create_tables import create_tables


class Command(BaseCommand):
    help = "Import all known Amsterdam schema files."
    requires_system_checks = False

    def add_arguments(self, parser):
        parser.add_argument("schema", nargs="*", help="Local schema files to import")
        parser.add_argument("--schema-url", default=settings.SCHEMA_URL)
        parser.add_argument(
            "--create-tables", dest="create_tables", action="store_true"
        )
        parser.add_argument(
            "--no-create-tables", dest="create_tables", action="store_false"
        )
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
        try:
            dataset = Dataset.objects.get(name=to_snake_case(schema.id))
        except Dataset.DoesNotExist:
            dataset = Dataset.create_for_schema(schema)
            self.stdout.write(f"  Created {name}")
            return dataset
        else:
            updated = dataset.save_for_schema(schema)
            if updated:
                self.stdout.write(f"  Updated {name}")
                return dataset

        return None
