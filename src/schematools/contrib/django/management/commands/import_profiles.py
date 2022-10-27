from typing import List, Optional

from django.conf import settings
from django.core.management import BaseCommand

from schematools import types
from schematools.contrib.django.models import Profile
from schematools.types import ProfileSchema
from schematools.utils import schemas_from_url


class Command(BaseCommand):
    help = "Import all known profiles from Amsterdam schema files."
    requires_system_checks = []

    def add_arguments(self, parser) -> None:
        parser.add_argument("profile", nargs="*", help="Local profile files to import")
        parser.add_argument(
            "--schema-url",
            default=settings.PROFILES_URL,
            help=f"Schema URL (default: {settings.PROFILES_URL})",
        )

    def handle(self, *args, **options) -> None:
        if options["profile"]:
            profiles = self.import_from_files(options["profile"])
        else:
            profiles = self.import_from_url(options["schema_url"])

        if profiles:
            self.stdout.write(f"Imported profiles: {len(profiles)}")
        else:
            self.stdout.write("No new profiles imported.")

    def import_from_files(self, profile_files: List[str]) -> List[Profile]:
        profiles = []
        for filename in profile_files:
            self.stdout.write(f"Loading profile from {filename}")
            schema = ProfileSchema.from_file(filename)
            profile = self._import(schema.name, schema)
            if profile is not None:
                profiles.append(profile)

        return profiles

    def import_from_url(self, schema_url: str) -> List[Profile]:
        """Import all schema definitions from an URL"""
        self.stdout.write(f"Loading profiles from {schema_url}")
        profiles = []

        for schema in schemas_from_url(
            base_url=schema_url, data_type=types.ProfileSchema
        ).values():
            self.stdout.write(f"* Processing {schema.name}")
            profile = self._import(schema)
            if profile is not None:
                profiles.append(profile)

        return profiles

    def _import(self, schema: ProfileSchema) -> Optional[Profile]:
        name = schema.name
        try:
            profile = Profile.objects.get(name=name)
        except Profile.DoesNotExist:
            profile = Profile.create_for_schema(schema)
            self.stdout.write(f"   Created {name}")
            return profile
        else:
            updated = profile.save_for_schema(schema)
            if updated:
                self.stdout.write(f"    Updated {name}")
                return profile

        return None
