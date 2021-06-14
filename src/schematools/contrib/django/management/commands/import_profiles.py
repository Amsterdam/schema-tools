from typing import List, Optional

from django.conf import settings
from django.core.cache import cache
from django.core.management import BaseCommand

from schematools.contrib.django.models import PROFILES_CACHE_KEY, Profile
from schematools.types import ProfileSchema
from schematools.utils import profile_defs_from_url


class Command(BaseCommand):
    help = "Import all known profiles from Amsterdam schema files."
    requires_system_checks = False

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
            cache.delete(PROFILES_CACHE_KEY)
            self.stdout.write(f"Imported profiles: {len(profiles)}")
        else:
            self.stdout.write("No new profiles imported.")

    def import_from_files(self, profile_files: List[str]) -> List[Profile]:
        profiles = []
        for filename in profile_files:
            self.stdout.write(f"Loading profile from {filename}")
            schema = ProfileSchema.from_file(filename)
            profile = self.import_schema(schema.name, schema)
            if profile is not None:
                profiles.append(profile)

        return profiles

    def import_from_url(self, schema_url: str) -> List[Profile]:
        """Import all schema definitions from an URL"""
        self.stdout.write(f"Loading profiles from {schema_url}")
        profiles = []

        for name, schema in profile_defs_from_url(schema_url).items():
            self.stdout.write(f"* Processing {name}")
            profile = self.import_schema(name, schema)
            if profile is not None:
                profiles.append(profile)

        return profiles

    def import_schema(self, name: str, schema: ProfileSchema) -> Optional[Profile]:
        try:
            profile = Profile.objects.get(name=schema.name)
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
