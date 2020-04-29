from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand

from schematools.contrib.django.models import Dataset
from schematools.types import DatasetSchema
from schematools.utils import schema_def_from_file

from .create_tables import create_tables


class Command(BaseCommand):
    help = "Import Amsterdam schema from file."
    requires_system_checks = False

    def add_arguments(self, parser):
        parser.add_argument("filename", type=str)

    def handle(self, *args, **options):
        schemas = schema_def_from_file(options["filename"])

        datasets = []
        for key, schema in schemas.items():
            datasets.append(self.import_schema(key, schema))

        if not datasets:
            self.stdout.write("No new datasets imported")
        else:
            create_tables(self, datasets, allow_unmanaged=True)

    def import_schema(self, name: str, schema: DatasetSchema) -> Optional[Dataset]:
        """Import a single dataset schema."""
        try:
            dataset = Dataset.objects.get(name=schema.id)
        except Dataset.DoesNotExist:
            dataset = Dataset.objects.create(
                name=schema.id, schema_data=schema.json_data()
            )
            self.stdout.write(f"  Created {name}")
            return dataset
        else:
            dataset.schema_data = schema.json_data()
            if dataset.schema_data_changed():
                dataset.save()
                self.stdout.write(f"  Updated {name}")
                return dataset

        return None
