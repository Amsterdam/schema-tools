from __future__ import annotations

import logging
from typing import List

from more_ds.network.url import URL

from schematools.contrib.django.models import Dataset
from schematools.contrib.django.schemas import get_schemas_for_url
from schematools.types import DatasetSchema
from schematools.utils import dataset_schema_from_path

logger = logging.getLogger(__name__)


def get_datasets_from_files(schema_files: List[str]) -> List[Dataset]:
    """Get dataset schemas for the given files."""
    schemas = [dataset_schema_from_path(filename) for filename in schema_files]
    return get_datasets_from_schemas(schemas)


def get_datasets_from_url(
    schema_url: URL, limit_to: List[str] | None = None, skip: List[str] | None = None
) -> List[Dataset]:
    """Get dataset schemas from a URL."""
    return get_datasets_from_schemas(get_schemas_for_url(schema_url, limit_to=limit_to, skip=skip))


def get_datasets_from_schemas(schemas: List[DatasetSchema]) -> List[Dataset]:
    """Get datasets for the given schemas."""
    datasets = []
    for schema in schemas:
        try:
            name = Dataset.name_from_schema(schema)
            dataset = Dataset.objects.get(name=name)
        except Dataset.DoesNotExist:
            logger.warning("Skipping schema: %s", name)
        else:
            datasets.append(dataset)
    return datasets
