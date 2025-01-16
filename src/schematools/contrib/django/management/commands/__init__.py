from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from django.conf import settings
from django.core.management import BaseCommand, CommandError, CommandParser

from schematools.contrib.django.models import Dataset


class BaseDatasetCommand(BaseCommand):
    """A management command that works on the list of imported datasets."""

    def add_arguments(self, parser: CommandParser) -> None:
        """Provide default arguments to pass dataset names to this management command."""
        parser.add_argument(
            "dataset", nargs="*", help="Datasets to use. Takes precedent over --datasets-list"
        )
        parser.add_argument(
            "--datasets-list",
            nargs="*",
            default=settings.DATASETS_LIST,
            help=f"Datasets to use (default: {settings.DATASETS_LIST})",
        )
        parser.add_argument(
            "-x",
            "--exclude",
            "--datasets-exclude",
            dest="datasets_exclude",
            nargs="*",
            default=settings.DATASETS_EXCLUDE,
            help="Datasets that need to be skipped. (default: {settings.DATASETS_EXCLUDE})",
        )

    def get_datasets(
        self, options: dict[str, Any], enable_db=None, default_all=False
    ) -> Iterable[Dataset]:
        """Provide the datasets based on the command options"""
        # Provide backwards compatibility for the positional argument datasets
        options["datasets_list"] = (
            options["dataset"] if options["dataset"] else options["datasets_list"]
        )

        if not options["datasets_list"] and not options["datasets_exclude"] and not default_all:
            raise CommandError(
                "Provide at least a dataset using --datasets-list, "
                "or use the --datasets-exclude option."
            )
        queryset = Dataset.objects.all()
        if enable_db is not None:
            queryset = queryset.filter(enable_db=enable_db)

        datasets = {ds.name for ds in queryset}
        if options["datasets_list"] is not None:
            datasets = set(options["datasets_list"])

        if options["datasets_exclude"] is not None:
            datasets = datasets - set(options["datasets_exclude"])

        queryset = queryset.filter(name__in=datasets)
        if invalid_names := datasets - {ds.name for ds in queryset}:
            raise CommandError(f"Datasets not found: {', '.join(sorted(invalid_names))}")

        return queryset
