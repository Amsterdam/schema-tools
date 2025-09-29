from __future__ import annotations

from django.conf import settings
from django.core.management import BaseCommand

from schematools.contrib.django.models import Scope
from schematools.loaders import get_scope_loader
from schematools.types import Scope as ScopeSchema


class Command(BaseCommand):
    help = "Import all known scopes from Amsterdam schema files."
    requires_system_checks = []

    def add_arguments(self, parser) -> None:
        parser.add_argument("scope", nargs="*", help="Local scope files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.SCOPES_URL,
            help=f"Schema URL (default: {settings.SCOPES_URL})",
        )

    def handle(self, *args, **options) -> None:
        if options["scope"]:
            scopes = self.import_from_files(options["scope"])
        else:
            scopes = self.import_from_url(options["schema_url"])

        if scopes:
            self.stdout.write(f"Imported scope: {len(scopes)}")
        else:
            self.stdout.write("No new scopes imported.")

    def import_from_files(self, scope_files: list[str]) -> list[Scope]:
        scopes = []
        for filename in scope_files:
            schema = ScopeSchema.from_file(filename)
            scope = self._import(schema)
            if scope is not None:
                scopes.append(scope)

        return scopes

    def import_from_url(self, schema_url: str) -> list[Scope]:
        """Import all schema definitions from an URL"""
        self.stdout.write(f"Loading scopes from {schema_url}")
        loader = get_scope_loader(schema_url)
        scopes = []

        for schema in loader.get_all_scopes():
            scope = self._import(schema)
            if scope is not None:
                scopes.append(scope)
        return scopes

    def _import(self, schema: ScopeSchema) -> Scope | None:
        self.stdout.write(f"* Processing {schema.name}")
        name = schema.name
        try:
            scope = Scope.objects.get(name=name)
        except Scope.DoesNotExist:
            scope = Scope.create_for_schema(schema)
            self.stdout.write(f"   Created {name}")
            return scope
        else:
            updated = scope.save_for_schema(schema)
            if updated:
                self.stdout.write(f"    Updated {name}")
                return scope

        return None
