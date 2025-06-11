from __future__ import annotations

from django.db import connection

from .factories import DjangoModelFactory
from .models import Dataset


def create_tables(dataset: Dataset):
    """Create the database tables for a given schema."""
    with connection.schema_editor() as schema_editor:
        factory = DjangoModelFactory(dataset)
        for model in factory.build_models():
            schema_editor.create_model(model)
