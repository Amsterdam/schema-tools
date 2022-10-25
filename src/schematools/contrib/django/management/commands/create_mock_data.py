from pathlib import Path
from typing import Any, Dict, List

from django.conf import settings
from django.core.management import BaseCommand, CommandParser

from schematools.contrib.django.datasets import get_datasets_from_files, get_datasets_from_url
from schematools.contrib.django.faker.create import create_data_for


class Command(BaseCommand):  # noqa: D101
    help = """Create mock data for Amsterdam schema files.

    Datasets (in DSO db) + dataset tables should already have been created,
    usually with the `import_schemas --create-tables` mgm. command.
    """  # noqa: A003
    requires_system_checks = []

    def add_arguments(self, parser: CommandParser) -> None:  # noqa: D102
        parser.add_argument("schema", nargs="*", help="Paths to local schema files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )
        parser.add_argument("-s", "--size", type=int, default=50, help="Number of rows")
        parser.add_argument("--sql", action="store_true", help="Generate the sql statements.")
        parser.add_argument(
            "--start-at", type=int, default=1, help="Starting number for sequences."
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
        path_based_datasets = []
        dataset_ids = []
        id_based_datasets = []
        paths_or_dataset_ids = options["schema"]

        if paths_or_dataset_ids:
            for path_or_dataset_id in paths_or_dataset_ids:
                if Path(path_or_dataset_id).exists():
                    paths.append(path_or_dataset_id)
                else:
                    dataset_ids.append(path_or_dataset_id)

        path_based_datasets = get_datasets_from_files(paths)

        if dataset_ids or not paths:
            id_based_datasets = get_datasets_from_url(
                options["schema_url"], limit_to=dataset_ids, skip=options["skip"]
            )

        sql_lines = create_data_for(
            *(path_based_datasets + id_based_datasets),
            start_at=options["start_at"],
            size=options["size"],
            sql=options["sql"],
        )
        if sql_lines:
            self.stdout.write("\n".join(sql_lines))
