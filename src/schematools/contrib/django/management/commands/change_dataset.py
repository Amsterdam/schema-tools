from argparse import ArgumentTypeError
from distutils.util import strtobool

from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError

from schematools.contrib.django.models import Dataset


def _strtobool(value):
    try:
        return bool(strtobool(value))
    except ValueError:
        raise ArgumentTypeError("expected boolean value") from None


class Command(BaseCommand):
    help = "Modify the settings for a dataset."
    requires_system_checks = False
    setting_options = (
        "auth",
        "enable_api",
        "enable_db",
        "endpoint_url",
        "ordering",
        "path",
    )

    def add_arguments(self, parser):
        parser.add_argument("dataset", help="Name of the dataset")
        parser.add_argument(
            "--enable-db",
            dest="enable_db",
            type=_strtobool,
            nargs="?",
            const=True,
            metavar="bool",
            help="Enable local database tables (default).",
        )
        parser.add_argument(
            "--enable-api",
            dest="enable_api",
            nargs="?",
            type=_strtobool,
            const=True,
            metavar="bool",
            help="Enable the API endpoint.",
        )
        parser.add_argument(
            "--enable-geosearch",
            dest="enable_geosearch",
            nargs="?",
            type=_strtobool,
            const=True,
            metavar="bool",
            help="Enable GeoSearch for all tables in dataset.",
        )
        parser.add_argument("--url-prefix", help="Set a prefix for the API URL.")
        parser.add_argument("--auth", help="Assign OAuth roles.")
        parser.add_argument("--ordering", type=int, help="Set the ordering of the dataset")
        parser.add_argument("--endpoint-url")

    def handle(self, *args, **options):
        name = options.pop("dataset")
        try:
            dataset = Dataset.objects.get(name=name)
        except Dataset.DoesNotExist:
            available = ", ".join(sorted(Dataset.objects.values_list("name", flat=True)))
            raise CommandError(f"Dataset not found: {name}.\nAvailable are: {available}") from None

        # Validate illogical combinations
        if options.get("endpoint_url"):
            if options.get("enable_db"):
                raise CommandError("Can't use --endpoint-url with --enable-db")

            if dataset.enable_db:
                options["enable_db"] = False

        changed = False
        if options.get("enable_geosearch") is not None:
            dataset.tables.all().update(enable_geosearch=options.get("enable_geosearch"))
            changed = True

        for field in self.setting_options:
            value = options.get(field)
            if value is None:
                continue

            if getattr(dataset, field) == value:
                self.stdout.write(f"dataset.{field} unchanged")
            else:
                self.stdout.write(f"Set dataset.{field}={value}")
                setattr(dataset, field, value)
                changed = True

        if changed:
            try:
                dataset.full_clean()
            except ValidationError as e:
                errors = []
                for field, messages in e.error_dict.items():
                    errors.extend(
                        [f"--{field.replace('_', '-')}: {err.message}" for err in messages]
                    )
                raise CommandError("Unable to save changes:\n" + "\n".join(errors)) from None

            dataset.save()
            self.stdout.write("The service needs to restart for changes to have effect.")
        else:
            self.stdout.write("No changes made.")
