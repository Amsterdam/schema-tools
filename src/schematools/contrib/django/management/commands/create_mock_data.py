from __future__ import annotations

from typing import Any

from django.core.management import CommandParser

from schematools.contrib.django.faker.create import create_data_for

from . import BaseDatasetCommand


class Command(BaseDatasetCommand):  # noqa: D101
    help = """Create mock data for Amsterdam schema files.

    Datasets (in DSO db) + dataset tables should already have been created,
    usually with the `import_schemas --create-tables` mgm. command.

    The mocking can be limited to tables defined in the `tables` option,
    however, this only makes sense when using a single dataset.
    """  # noqa: A003
    requires_system_checks = []

    def add_arguments(self, parser: CommandParser) -> None:  # noqa: D102
        super().add_arguments(parser)
        parser.add_argument("-s", "--size", type=int, default=50, help="Number of rows")
        parser.add_argument("--sql", action="store_true", help="Generate the sql statements.")
        parser.add_argument(
            "--start-at", type=int, default=1, help="Starting number for sequences."
        )
        parser.add_argument("--table", "-t", nargs="*", help="Names of tables.")

    def handle(self, *args: list[Any], **options: dict[str, Any]) -> None:  # noqa: D102
        datasets = self.get_datasets(options, enable_db=True, default_all=False)
        if len(datasets) > 1 and options["table"]:
            self.stdout.write("The `tables` options can only be used with one dataset.")
            return
        sql_lines = create_data_for(
            *datasets,
            start_at=options["start_at"],
            size=options["size"],
            sql=options["sql"],
            tables=options["table"],
        )
        if sql_lines:
            self.stdout.write("\n".join(sql_lines))
