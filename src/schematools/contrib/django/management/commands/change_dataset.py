from __future__ import annotations

import argparse
from argparse import ArgumentTypeError
from typing import Any

from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, CommandError

from schematools.contrib.django.models import Dataset


def _strtobool(value: str) -> bool:
    """Convert a string representation of truth to True or False.

    Reimplement strtobool per PEP 632 and python 3.12 deprecation

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    value = value.lower()
    if value in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif value in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ArgumentTypeError("expected boolean value") from None


class Command(BaseCommand):  # noqa: D101
    help = "Modify the settings for a dataset."  # noqa: A003
    requires_system_checks = []
    setting_options = (
        "auth",
        "enable_api",
        "enable_db",
        "endpoint_url",
        "ordering",
        "path",
    )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:  # noqa: D102
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

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: D102
        name = options.pop("dataset")
        try:
            dataset = Dataset.objects.get(name=name)
        except Dataset.DoesNotExist:
            available = ", ".join(sorted(Dataset.objects.values_list("name", flat=True)))
            raise CommandError(f"Dataset not found: {name}.\nAvailable are: {available}") from None

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
