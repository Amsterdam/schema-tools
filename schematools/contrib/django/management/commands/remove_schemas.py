from django.core.management import BaseCommand

from schematools.contrib.django.models import Dataset


class Command(BaseCommand):
    help = "Drop the specified schemas"
    requires_system_checks = False

    def add_arguments(self, parser):
        parser.add_argument(
            "schemas",
            nargs="+",
            help="Schemas that need to be dropped",
        )

    def handle(self, *args, **options):
        datasets = Dataset.objects.all()
        imported_datasets = {d.name for d in datasets}
        drop_schemas = set(options.get("schemas", []))
        if not drop_schemas <= imported_datasets:
            impossible_schemas = drop_schemas - imported_datasets
            raise ValueError(f"These schemas do not exist: {impossible_schemas}")

        datasets.filter(name__in=drop_schemas).delete()
        if options["verbosity"] > 0:
            self.stdout.write(f"Deleted the following datasets: {drop_schemas}")
