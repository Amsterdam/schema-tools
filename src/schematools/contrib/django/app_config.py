from __future__ import annotations

import logging
import os
import sys
from types import ModuleType

from django.apps import apps
from django.apps import config as apps_config
from django.apps.registry import Apps
from django.conf import settings
from django.db import models

logger = logging.getLogger(__name__)


class VirtualAppConfig(apps_config.AppConfig):
    """Virtual App Config, allowing to add models for datasets on the fly."""

    def __init__(self, apps: Apps, app_name: str, app_module: ModuleType):
        """Patch this object for virtual apps."""
        super().__init__(app_name, app_module)
        # Make django think that App is initiated already.
        self.models: dict[str, type[models.Model]] = {}
        # Path is required for Django to think this APP is real.
        self.path = os.path.dirname(__file__)
        self.apps = apps

        # Disable migrations for this model.
        if not hasattr(settings, "MIGRATION_MODULES"):
            settings.MIGRATION_MODULES = {}
        try:
            settings.MIGRATION_MODULES[self.label] = None
        except TypeError as e:
            if settings.MIGRATION_MODULES.__class__.__name__ != "DisableMigrations":
                # pytest_django.migrations.DisableMigrations does not allow item assignment,
                # but doesn't need it either. Only warn when migrations might run
                msg = str(e)
                logger.warning("Failed to disable migrations for %s: %s", self.label, msg)

        self.ready()

    def _path_from_module(self, module: ModuleType) -> str | None:
        """Disable OS loading for this App Config."""
        return None

    def register_model(self, model: type[models.Model]) -> None:
        """Register model in django registry and update models."""
        self.apps.register_model(self.label, model)
        self.models = self.apps.all_models[self.label]


def register_model(dataset_id: str, model: type[models.Model]) -> None:
    """Register the model in django.apps."""
    try:
        app_config = apps.app_configs[dataset_id]
    except KeyError:
        # Insert a new virtual "app_label" into the Django app registry,
        # so foreign key relations can be resolved. based on the dataset id.
        app_module = sys.modules[__name__]
        app_config = VirtualAppConfig(apps, dataset_id, app_module=app_module)
        apps.app_configs[dataset_id] = app_config

    app_config.register_model(model)
