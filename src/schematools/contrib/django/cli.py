import sys

import django
import environ
from django.conf import settings

env = environ.Env()

settings.configure(
    DATABASES={"default": env.db_url("DATABASE_URL")},
    DEBUG=True,
    INSTALLED_APPS=["schematools.contrib.django"],
    SCHEMA_URL=env.str("SCHEMA_URL"),
    AMSTERDAM_SCHEMA={"geosearch_disabled_datasets": ["bag"]},
    SCHEMA_DEFS_URL=env.str("SCHEMA_DEFS_URL", "https://schemas.data.amsterdam.nl/schema"),
)
django.setup()


def main():
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)
