from django.db import connection

from schematools.types import DatasetSchema
from .factories import schema_models_factory


def create_tables(dataset: DatasetSchema, tables=None):
    """Create the database tables for a given schema."""
    with connection.schema_editor() as schema_editor:
        for model in schema_models_factory(dataset, tables=tables):
            schema_editor.create_model(model)
