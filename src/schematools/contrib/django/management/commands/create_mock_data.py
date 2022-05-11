from typing import List

from django.conf import settings
from django.core.management import BaseCommand

from schematools.contrib.django.factories import schema_model_mockers_factory
from schematools.contrib.django.models import Dataset
from schematools.types import DatasetSchema
from schematools.utils import dataset_schema_from_path, dataset_schemas_from_url


class Command(BaseCommand):  # noqa: D101
    help = """Create mock data for Amsterdam schema files.

    Datasets (in DSO db) + dataset tables should already have been created,
    usually with the `import_schemas --create-tables` mgm. command.
    """  # noqa: A003
    requires_system_checks = False

    def add_arguments(self, parser):  # noqa: D102
        parser.add_argument("schema", nargs="*", help="Paths to local schema files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument("-s", "--size", type=int, default=50, help="Number of rows")

    def handle(self, *args, **options):  # noqa: D102

        if options["schema"]:
            datasets = self.get_datasets_from_files(options["schema"])
        else:
            datasets = self.get_datasets_from_url(options["schema_url"])

        size = options["size"]

        for dataset in datasets:
            model_mockers = {
                cls._meta.get_model_class()._meta.model_name: cls
                for cls in schema_model_mockers_factory(
                    dataset, base_app_name="dso_api.dynamic_api"
                )
            }

            for table in dataset.tables.all():
                model_mockers[table.name].create_batch(size)

    def get_datasets_from_files(self, schema_files) -> List[Dataset]:
        """Get dataset schemas for the given files."""
        schemas = [dataset_schema_from_path(filename) for filename in schema_files]
        return self.get_datasets_from_schemas(schemas)

    def get_datasets_from_url(self, schema_url) -> List[Dataset]:
        """Get dataset schemas from a URL."""
        return self.get_datasets_from_schemas(dataset_schemas_from_url(schema_url).values())

    def get_datasets_from_schemas(self, schemas: List[DatasetSchema]) -> List[Dataset]:
        """Get datasets for the given schemas."""
        datasets = []
        for schema in schemas:
            try:
                name = Dataset.name_from_schema(schema)
                dataset = Dataset.objects.get(name=name)
            except Dataset.DoesNotExist:
                self.stdout.write(f"Warning: skipping schema: {name}")
            datasets.append(dataset)
        return datasets
