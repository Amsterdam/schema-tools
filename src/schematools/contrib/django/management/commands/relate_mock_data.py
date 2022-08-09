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
        parser.add_argument("--sql", action="store_true", help="Generate the sql statements.")
        parser.add_argument("--skip", nargs="*", default=[], help="Dataset ids to be skipped.")
        parser.add_argument(
            "--limit_to", nargs="*", default=[], help="Dataset ids to be included exclusively."
        )

    def handle(self, *args: List[Any], **options: Dict[str, Any]) -> None:  # noqa: D102

        if options["schema"]:
            schemas = [dataset_schema_from_path(filename) for filename in list(options["schema"])]
        else:
            schemas = get_schemas_for_url(
                options["schema_url"], limit_to=options["limit_to"], skip=options["skip"]
            )

        as_sql = options["sql"]
        sql_lines = relate_datasets(*schemas, as_sql=as_sql)
        if as_sql:
            self.stdout.write("\n".join(sql_lines))
