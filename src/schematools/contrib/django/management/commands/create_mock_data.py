from pathlib import Path
from typing import Any, Dict, List

from django.conf import settings
from django.core.management import BaseCommand, CommandParser

from schematools.contrib.django.datasets import get_datasets_from_files, get_datasets_from_url
from schematools.contrib.django.faker.create import create_data_for

from .parsing import group_dataset_args


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
            "-x",
            "--exclude",
            action="store_true",
            help="""If `exclude` is defined, all schemas found at `SCHEMA_URL` are processed and
            the schemas defined as positional arguments are excluded""",
        )

    def handle(self, *args: List[Any], **options: Dict[str, Any]) -> None:  # noqa: D102

        id_based_datasets = []
        to_be_skipped = []
        paths, dataset_ids = group_dataset_args(options["schema"])
        exclude = options["exclude"]
        if exclude:
            to_be_skipped = dataset_ids
            dataset_ids = []

        if exclude and paths:
            raise ValueError("Path-based schemas are not compatible with `--exclude`.")

        path_based_datasets = get_datasets_from_files(paths)

        if dataset_ids or not paths:
            id_based_datasets = get_datasets_from_url(
                options["schema_url"], limit_to=dataset_ids, skip=to_be_skipped
            )

        sql_lines = create_data_for(
            *(path_based_datasets + id_based_datasets),
            start_at=options["start_at"],
            size=options["size"],
            sql=options["sql"],
        )
        if sql_lines:
            self.stdout.write("\n".join(sql_lines))
