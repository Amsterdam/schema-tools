from __future__ import annotations

from datetime import timedelta

from django.core.management import BaseCommand
from django.db import connection
from django.db.utils import ProgrammingError
from django.utils import timezone
from psycopg import sql

from schematools.contrib.django.models import Dataset, DatasetTable


class Command(BaseCommand):
    help = (
        "Delete datasets and tables whose delete_date is more than 29 days in the past"
    )

    def handle(self, *args, **options):
        cutoff = timezone.now() - timedelta(days=30)

        expired_datasets = Dataset.objects.filter(
            delete_date__isnull=False,
            delete_date__lte=cutoff,
        )

        if not expired_datasets.exists():
            self.stdout.write("No expired schemas found.")
            return

        for dataset in expired_datasets:
            tables = DatasetTable.objects.filter(name=dataset)

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
            # Delete datasets
            dataset.delete()
            if options["verbosity"] > 0:
                self.stdout.write(
                    f"Deleted datasets {', '.join(ds.name for ds in expired_datasets)}"
                )
