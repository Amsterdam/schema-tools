from __future__ import annotations

from django.apps import AppConfig
from django.db import models

# Make sure OneToOneField also allows __contains= lookups (will only work for type=string).
# This allows DjangoModelFactory.build_model() to create the CheckConstraint check for a
# relational PK field.
models.OneToOneField.register_lookup(models.lookups.Contains)


class SchematoolsAppConfig(AppConfig):
    name = "schematools.contrib.django"
    # Alias as `datasets` app to avoid unnecessary migrations on legacy systems.
    label = "datasets"
    default_auto_field = "django.db.models.BigAutoField"
