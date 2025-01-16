from __future__ import annotations

import logging

from django.db import connection
from django.db.models import sql

from schematools.contrib.django.factories import schema_model_mockers_factory
from schematools.contrib.django.models import Dataset, DynamicModel

logger = logging.getLogger(__name__)


def create_data_for(
    *datasets: Dataset,
    start_at: int = 1,
    size: int = 50,
    sql: bool = False,
    tables: list[str] | None = None,
) -> list[str]:
    """Create mock data for the indicated datasets."""
    limit_tables_to = set(tables) if tables is not None else set()
    model_mockers = {}
    for dataset in datasets:
        if not dataset.enable_db:
            logger.warning("Skipping `%s`, `enable_db` is False", dataset.name)
            continue
        model_mockers.update(
            {
                cls._meta.get_model_class()._meta.model_name: cls
                for cls in schema_model_mockers_factory(
                    dataset, base_app_name="dso_api.dynamic_api"
                )
            }
        )

    # After all model mocks are created, start creating data to make sure relations are available
    for mock_model in model_mockers.values():
        if limit_tables_to and mock_model._meta.model.__name__ not in limit_tables_to:
            continue
        if start_at > 1:
            mock_model._setup_next_sequence = lambda: start_at
        if sql:
            return list(_get_sql_for(mock_model.build_batch(size)))
        else:
            mock_model.create_batch(size)
    return None


def _get_sql_for(objects: list[DynamicModel]):
    """Get the SQL insert statements for the provided model objects."""
    # We need a real cursor here, so that `cursor.mogrify`
    # knows exactly how to render the query.
    cursor = connection.cursor()
    for obj in objects:
        values = obj._meta.local_fields
        query = sql.InsertQuery(obj)
        query.insert_values(values, [obj])
        compiler = query.get_compiler("default")
        statements = compiler.as_sql()
        for statement, params in statements:
            yield cursor.mogrify(statement, params).decode() + ";"
