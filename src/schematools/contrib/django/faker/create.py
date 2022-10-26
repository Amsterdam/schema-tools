import logging
from typing import List

from schematools.contrib.django.factories import schema_model_mockers_factory
from schematools.contrib.django.models import Dataset
from schematools.contrib.django.sql import get_sql_for

logger = logging.getLogger(__name__)


def create_data_for(
    *datasets: List[Dataset], start_at: int = 1, size: int = 50, sql: bool = False
) -> List[str]:
    """Create mock data for the indicated datasets."""
    for dataset in datasets:
        if not dataset.enable_db:
            logger.warning("Skipping `%s`, `enable_db` is False", dataset.name)
            continue
        model_mockers = {
            cls._meta.get_model_class()._meta.model_name: cls
            for cls in schema_model_mockers_factory(dataset, base_app_name="dso_api.dynamic_api")
        }

        for mock_model in model_mockers.values():
            if start_at > 1:
                mock_model._setup_next_sequence = lambda: start_at
            if sql:
                return get_sql_for(mock_model.build_batch(size))
            else:
                mock_model.create_batch(size)
