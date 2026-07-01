from __future__ import annotations

import io

from django.core.management import BaseCommand, CommandError
from django.utils import timezone

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

    def handle(self, *args, **options):
        """Django hook implementing command logic."""
        datasets = Dataset.objects.all()
        imported_datasets = {d.name for d in datasets}
        drop_schemas = set(options.get("schemas", []))

        impossible_schemas = [
            ident for ident in drop_schemas if ident not in imported_datasets
        ]
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
        tables = DatasetTable.objects.filter(dataset__in=dataset_qs)

        # set delete date to soft delete tables and datasets
        for table in tables:
            name = table.db_table
            if table.delete_date is None:
                table.delete_date = timezone.now()
                table.save(update_fields=["delete_date"])
                self.stdout.write(f"Added delete date to table {name}")

        for dataset in dataset_qs:
            dataset.delete_date = timezone.now()
            dataset.save(update_fields=["delete_date"])
            self.stdout.write(f"Added delete date to dataset {dataset}")
