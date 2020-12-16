import environ
from django.conf import settings

env = environ.Env()

settings.configure(
    DATABASES={"default": env.db_url("DATABASE_URL")},
    DEBUG=True,
    INSTALLED_APPS=["schematools.contrib.django"],
    SCHEMA_URL=env.str("SCHEMA_URL"),
    AMSTERDAM_SCHEMA={"geosearch_disabled_datasets": ["bag"]},
    SCHEMA_DEFS_URL=env.str(
        "SCHEMA_DEFS_URL", "https://schemas.data.amsterdam.nl/schema"
    ),
)
