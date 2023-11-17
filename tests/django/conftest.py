"""Extra fixtures for ``schematools.contrib.django``"""
from django.conf import settings
from pytest_django.plugin import _setup_django


def pytest_configure(config):
    import environ

    env = environ.Env()
    databases = {
        "default": env.db_url(
            "DATABASE_URL",
            default="postgresql://dataservices:insecure@localhost:5415/dataservices",
            engine="django.contrib.gis.db.backends.postgis",
        ),
    }
    databases["default"]["NAME"] += "_django"  # avoid duplication with sqlalchemy tests
    settings.configure(  # noqa: S106
        DEBUG=True,
        INSTALLED_APPS=[
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "django.contrib.staticfiles",
            "django.contrib.gis",
            "django.contrib.postgres",
            "django_db_comments",
            "schematools.contrib.django",
        ],
        DATABASES=databases,
        CACHES={
            "default": {
                "BACKEND": "django.core.cache.backends.dummy.DummyCache",
            }
        },
        CSRF_COOKIE_SECURE=False,
        SESSION_COOKIE_SECURE=False,
        SECRET_KEY="PYTEST",
        PASSWORD_HASHERS=["django.contrib.auth.hashers.MD5PasswordHasher"],
        SCHEMA_URL=env.str("SCHEMA_URL", "https://schemas.data.amsterdam.nl/datasets/"),
        SCHEMA_DEFS_URL=env.str("SCHEMA_DEFS_URL", "https://schemas.data.amsterdam.nl/schema"),
        AMSTERDAM_SCHEMA={"geosearch_disabled_datasets": []},
    )
    _setup_django(config)

    import tests.django.fixtures

    config.pluginmanager.register(tests.django.fixtures)
