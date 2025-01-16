from __future__ import annotations

from django.db import ProgrammingError, connection

from . import BaseDatasetCommand


class Command(BaseDatasetCommand):  # noqa: D101
    help = "Truncates the tables of a given dataset."
    requires_system_checks = []  # don't test URLs (which create models)

    def handle(self, *args, **options):  # noqa: D102
        db_tables = []
        for dataset in self.get_datasets(options, enable_db=True, default_all=False):
            db_tables.extend(
                table.db_name
                for table in dataset.schema.get_tables(include_nested=True, include_through=True)
            )

        with connection.cursor() as cursor:
            for db_table in sorted(db_tables):
                self.stdout.write(f"Truncating {db_table}")
                try:
                    cursor.execute(f"TRUNCATE {db_table}")
                except ProgrammingError:
                    # Catch missing tables, happens when views aren't generated on import_schemas
                    self.stdout.write(f"Failed to truncate {db_table}")
