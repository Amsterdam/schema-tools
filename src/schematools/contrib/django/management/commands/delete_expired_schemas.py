from __future__ import annotations

from datetime import timedelta

from django.core.management import BaseCommand, call_command
from django.utils import timezone

from schematools.contrib.django.models import Dataset


class Command(BaseCommand):
    help = "Delete datasets and tables whose delete_date is more than 30 days old"

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

            call_command(
                "remove_schemas",
                dataset.name,
                hard_delete=True,
            )

        self.stdout.write(
            f"Finished processing {expired_datasets.count()} expired schema(s)"
        )
