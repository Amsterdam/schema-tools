from typing import Any, Dict, List

from django.conf import settings
from django.core.management import BaseCommand, CommandParser

from schematools.contrib.django.faker.relate import relate_datasets
from schematools.contrib.django.schemas import get_schemas_for_url
from schematools.utils import dataset_schema_from_path

from .parsing import group_dataset_args


class Command(BaseCommand):  # noqa: D101
    help = """Relate mock records.

    When mock data is created, the relations are filled with `null` values.
    Using this command, the relations can be added to the records.
    """  # noqa: A003
    requires_system_checks = []

    def add_arguments(self, parser: CommandParser) -> None:  # noqa: D102
        parser.add_argument("schema", nargs="*", help="Paths to local schema files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument(
            "-x",
            "--exclude",
            action="store_true",
            help="""If `exclude` is defined, all schemas found at `SCHEMA_URL` are processed and
            the schemas defined as positional arguments are excluded""",
        )

    def handle(self, *args: List[Any], **options: Dict[str, Any]) -> None:  # noqa: D102

        path_based_schemas = []
        id_based_schemas = []
        to_be_skipped = []
        paths, dataset_ids = group_dataset_args(options["schema"])
        exclude = options["exclude"]
        if exclude:
            to_be_skipped = dataset_ids
            dataset_ids = []

        if exclude and paths:
            raise ValueError("Path-based schemas are not compatible with `--exclude`.")

        if paths:
            path_based_schemas = [dataset_schema_from_path(path) for path in paths]

        if dataset_ids or not paths:
            id_based_schemas = get_schemas_for_url(
                options["schema_url"], limit_to=dataset_ids, skip=to_be_skipped
            )

        relate_datasets(*(path_based_schemas + id_based_schemas))
