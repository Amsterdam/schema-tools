from __future__ import annotations

from collections.abc import Collection

from django.db import connection

from .factories import schema_models_factory
from .models import Dataset


def create_tables(
    dataset: Dataset,
    tables: Collection[str] | None = None,
    base_app_name: str | None = None,
):
    """Create the database tables for a given schema."""
    with connection.schema_editor() as schema_editor:
        for model in schema_models_factory(dataset, tables=tables, base_app_name=base_app_name):
            schema_editor.create_model(model)
