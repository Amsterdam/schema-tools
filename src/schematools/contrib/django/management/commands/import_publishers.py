from __future__ import annotations

from django.conf import settings
from django.core.management import BaseCommand

from schematools.contrib.django.models import Publisher
from schematools.loaders import get_schema_loader
from schematools.types import Publisher as PublisherSchema


class Command(BaseCommand):
    help = "Import all known publishers from Amsterdam schema files."
    requires_system_checks = []

    def add_arguments(self, parser) -> None:
        parser.add_argument("publisher", nargs="*", help="Local publisher files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCHEMA_URL,
            help=f"Schema URL (default: {settings.SCHEMA_URL})",
        )

    def handle(self, *args, **options) -> None:
        if options["publisher"]:
            publishers = self.import_from_files(options["publisher"])
        else:
            publishers = self.import_from_url(options["schema_url"])

        if publishers:
            self.stdout.write(f"Imported publishers: {len(publishers)}")
        else:
            self.stdout.write("No new publishers imported.")

    def import_from_files(self, publisher_files: list[str]) -> list[Publisher]:
        publishers = []
        for filename in publisher_files:
            schema = PublisherSchema.from_file(filename)
            publisher = self._import(schema)
            if publisher is not None:
                publishers.append(publisher)

        return publishers

    def import_from_url(self, schema_url: str) -> list[Publisher]:
        """Import all schema definitions from an URL"""
        self.stdout.write(f"Loading publishers from {schema_url}")
        loader = get_schema_loader(schema_url)
        publishers = []

        for _, schema in loader.get_all_publishers().items():
            publisher = self._import(schema)
            if publisher is not None:
                publishers.append(publisher)
        return publishers

    def _import(self, schema: PublisherSchema) -> Publisher | None:
        self.stdout.write(f"* Processing {schema.name}")
        id = schema.id
        try:
            publisher = Publisher.objects.get(id=id)
        except Publisher.DoesNotExist:
            publisher = Publisher.create_for_schema(schema)
            self.stdout.write(f"   Created {schema.name}")
            return publisher
        else:
            updated = publisher.save_for_schema(schema)
            if updated:
                self.stdout.write(f"    Updated {schema.name}")
                return publisher

        return None
