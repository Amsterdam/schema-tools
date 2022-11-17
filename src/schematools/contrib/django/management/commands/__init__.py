from typing import Any, Iterable

from django.core.management import BaseCommand, CommandError, CommandParser

from schematools.contrib.django.models import Dataset


class BaseDatasetCommand(BaseCommand):
    """A management command that works on the list of imported datasets."""

    def add_arguments(self, parser: CommandParser) -> None:
        """Provide default arguments to pass dataset names to this management command."""
        parser.add_argument("dataset", nargs="*", help="Names of the datasets.")
        parser.add_argument(
            "-x",
            "--exclude",
            dest="exclude",
            nargs="*",
            default=[],
            help="Datasets that need to be skipped.",
        )

    def get_datasets(
        self, options: dict[str, Any], enable_db=None, default_all=False
    ) -> Iterable[Dataset]:
        """Provide the datasets based on the command options"""
        datasets = Dataset.objects.all()
        if enable_db is not None:
            datasets = datasets.filter(enable_db=enable_db)

        if options["dataset"]:
            names = set(options["dataset"]) - set(options["exclude"])
            datasets = datasets.filter(name__in=names)
            if invalid_names := names - set(ds.name for ds in datasets):
                raise CommandError(f"Datasets not found: {', '.join(sorted(invalid_names))}")
        elif options["exclude"]:
            datasets = datasets.exclude(name__in=options["exclude"])
        elif not default_all:
            raise CommandError("Provide at least a dataset by name, or use the --exclude option.")

        return datasets
