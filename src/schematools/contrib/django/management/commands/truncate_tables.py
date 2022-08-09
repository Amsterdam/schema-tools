from django.conf import settings
from django.core.management import BaseCommand
from django.db import connection

from schematools.contrib.django.datasets import get_datasets_from_files, get_datasets_from_url
from schematools.contrib.django.factories import schema_models_factory


class Command(BaseCommand):  # noqa: D101
    help = "Truncates the tables based on the uploaded Amsterdam schema's."  # noqa: A003
    requires_system_checks = []  # don't test URLs (which create models)

    def add_arguments(self, parser):  # noqa: D102
        parser.add_argument("schema", nargs="*", help="Paths to local schema files")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument(
            "--skip",
            dest="skip",
            nargs="*",
            default=[],
            help="Schemas that need to be skipped",
        )

    def handle(self, *args, **options):  # noqa: D102
        skip = set(options["skip"])
        if options["schema"]:
            datasets = get_datasets_from_files(list(options["schema"]))
        else:
            datasets = get_datasets_from_url(options["schema_url"])

        models = []
        for dataset in datasets:
            if not dataset.enable_db or dataset.name in skip:
                continue
            models.extend(schema_models_factory(dataset))

        with connection.cursor() as cursor:
            for model in models:
                cursor.execute(f"TRUNCATE {model._meta.db_table}")
