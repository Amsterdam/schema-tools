# from dso_api.settings import *
import environ

env = environ.Env()
DEBUG = True

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "django.contrib.gis",
    "django.contrib.postgres",
    "schematools.contrib.django",
]

DATABASES = {
    "default": env.db_url(
        "DATABASE_URL",
        default="postgres://dataservices:insecure@localhost:5415/dataservices",
        engine="django.contrib.gis.db.backends.postgis",
    ),
}

CSRF_COOKIE_SECURE = False
SESSION_COOKIE_SECURE = False
SECRET_KEY = "PYTEST"

PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
