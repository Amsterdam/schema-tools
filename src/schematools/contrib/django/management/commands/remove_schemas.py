from __future__ import annotations

import io

from django.core.management import BaseCommand, CommandError
from django.db import connection
from django.db.utils import ProgrammingError
from psycopg import sql

from schematools.contrib.django.models import Dataset, DatasetTable
from schematools.naming import to_snake_case


class Command(BaseCommand):
    """Remove Dataset objects and optionally their underlying tables."""

    help = "Drop the specified schemas"  # noqa: A003
    requires_system_checks = []

    def add_arguments(self, parser):
        """Django hook."""
        parser.add_argument(
            "schemas",
            nargs="+",
            help="Schemas that need to be dropped",
        )
        parser.add_argument(
            "--drop-tables",
            action="store_true",
            default=False,
            help="Also drop the tables associated with the schemas",
        )

    def handle(self, *args, **options):
        """Django hook implementing command logic."""
        datasets = Dataset.objects.all()
        imported_datasets = {d.name for d in datasets}
        drop_schemas = set(options.get("schemas", []))

        impossible_schemas = [ident for ident in drop_schemas if ident not in imported_datasets]
        if impossible_schemas:
            msg = io.StringIO()
            msg.write("These schemas do not exist:\n")

            for ident in impossible_schemas:
                msg.write(f"* {ident!r}")
                snake = to_snake_case(ident)
                if snake in imported_datasets:
                    msg.write(f" (did you mean {snake!r}?)")
                msg.write("\n")

            raise CommandError(msg.getvalue())

        dataset_qs = datasets.filter(name__in=drop_schemas)
        if options["drop_tables"]:
            tables = DatasetTable.objects.filter(dataset__in=dataset_qs)
            with connection.cursor() as cursor:
                for table in tables:
                    name = table.db_table
                    try:
                        cursor.execute(
                            sql.SQL("DROP TABLE {table} CASCADE;").format(
                                table=sql.Identifier(name)
                            )
                        )
                        self.stdout.write(f"Deleted table {name}")
                    except ProgrammingError as e:
                        # Dataset(Table) descriptions and their generated tables
                        # exist independently. It is therefore possible
                        # that the table is already deleted or does not exist.
                        # We only need to attempt to clean it here.
                        self.stdout.write(f"Failed to delete table {name}. Error: {e}")
                        continue

        dataset_qs.delete()
        if options["verbosity"] > 0:
            self.stdout.write(f"Deleted the following datasets: {drop_schemas}")
