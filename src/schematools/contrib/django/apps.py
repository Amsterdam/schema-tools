from django.apps import AppConfig


class SchematoolsAppConfig(AppConfig):
    name = "schematools.contrib.django"
    # Alias as `datasets` app to avoid unnecessary migrations on legacy systems.
    label = "datasets"
