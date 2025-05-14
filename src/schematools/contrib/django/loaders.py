from __future__ import annotations

import json
import logging

from schematools.contrib.django.models import Dataset
from schematools.exceptions import DatasetNotFound
from schematools.loaders import CachedSchemaLoader
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema, DatasetTableSchema

logger = logging.getLogger(__name__)


class DatabaseSchemaLoader(CachedSchemaLoader):
    """A schema loader that retrieves datasets that are imported into the Django database.

    When objects are directly retrieved using ``Dataset.objects.get()``,
    there is no linking of schemas between different schema's.
    This loader interface makes sure schema's can resolve their relations by fetching
    the relevant definitions from the database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_all_datasets(self, enable_db=None) -> dict[str, DatasetSchema]:
        """Retrieve all datasets from the database storage"""
        queryset = Dataset.objects.all()
        if enable_db is not None:
            queryset = queryset.filter(enable_db=enable_db)

        return {
            path: self._as_dataset(schema_data, view_sql)
            for path, schema_data, view_sql in queryset.values_list(
                "path", "schema_data", "view_data"
            )
        }

    def _get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Retrieve a single dataset from the database storage."""
        queryset = Dataset.objects.filter(name=to_snake_case(dataset_id))
        try:
            schema_data = queryset.values_list("schema_data", flat=True)[0]
            view_sql = queryset.values_list("view_data", flat=True)[0]
        except IndexError:
            raise DatasetNotFound(f"Dataset `{dataset_id}` not found.") from None

        # The dataset is connected to this collection so it can resolve relations:
        return self._as_dataset(schema_data, view_sql)

    def _as_dataset(self, schema_data: str, view_sql: str | None = None) -> DatasetSchema:
        """Convert the retrieved schema into a real object that can resolve its relations."""
        return DatasetSchema(json.loads(schema_data), view_sql, loader=self)

    def _get_table(self, dataset: DatasetSchema, table_ref) -> DatasetTableSchema:
        """Datasets have their tables inlined, so there is no need to support this either."""
        raise NotImplementedError("DatabaseSchemaLoader don't support versioned tables")
