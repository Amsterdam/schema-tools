from pathlib import Path
from typing import Any, Dict, List

from django.conf import settings
from django.core.management import BaseCommand, CommandParser

from schematools.contrib.django.faker.relate import relate_datasets
from schematools.contrib.django.schemas import get_schemas_for_url
from schematools.utils import dataset_schema_from_path


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
            "--skip",
            nargs="*",
            default=[],
            help="""Dataset ids to be skipped. Only applies to id-based dataset,
            not to path-based dataset. Use a list of ids, e.g.: --skip bag fietspaaltjes""",
        )

    def handle(self, *args: List[Any], **options: Dict[str, Any]) -> None:  # noqa: D102

        paths = []
        path_based_schemas = []
        dataset_ids = []
        id_based_schemas = []
        paths_or_dataset_ids = options["schema"]

        if paths_or_dataset_ids:
            for path_or_dataset_id in paths_or_dataset_ids:
                if Path(path_or_dataset_id).exists():
                    paths.append(path_or_dataset_id)
                else:
                    dataset_ids.append(path_or_dataset_id)

            path_based_schemas = [dataset_schema_from_path(path) for path in paths]

        if dataset_ids or not paths:
            id_based_schemas = get_schemas_for_url(
                options["schema_url"], limit_to=dataset_ids, skip=options["skip"]
            )

        relate_datasets(*(path_based_schemas + id_based_schemas))
