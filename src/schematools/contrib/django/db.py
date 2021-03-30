from typing import Collection, Optional

from django.db import connection

from .factories import schema_models_factory
from .models import Dataset


def create_tables(
    dataset: Dataset,
    tables: Optional[Collection[str]] = None,
    base_app_name: Optional[str] = None,
):
    """Create the database tables for a given schema."""
    with connection.schema_editor() as schema_editor:
        for model in schema_models_factory(dataset, tables=tables, base_app_name=base_app_name):
            schema_editor.create_model(model)
