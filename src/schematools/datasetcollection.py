from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

from simple_singleton import Singleton

if TYPE_CHECKING:
    from schematools.types import DatasetSchema


class DatasetCollection(metaclass=Singleton):
    """Holding class for a collection of datasets.

    This can hold a cache of datasets that have already been collected.
    This class is a singleton class, so every DatasetSchema can have
    a reference to it, without creating redundancy.
    """

    def __init__(self) -> None:
        self.datasets_cache = {}

    def _load_dataset(self, dataset_id: str) -> Optional[DatasetSchema]:
        """Loads the dataset from SCHEMA_URL.

        If SCHEMA_URL is not defined, return None.
        """
        # Avoid circular import problem.
        from schematools.utils import schema_def_from_url

        try:
            schemas_url = os.environ["SCHEMA_URL"]
        except KeyError:
            return None

        return schema_def_from_url(schemas_url, dataset_id, prefetch_related=True)

    def add_dataset(self, dataset: DatasetSchema) -> None:
        self.datasets_cache[dataset.id] = dataset

    def get_dataset(self, dataset_id: str) -> DatasetSchema:
        """Gets a dataset by id from the cache.

        If not available, load the dataset from the SCHEMA_URL location.
        NB. Because dataset schemas are imported into the Postgresql database
        by the DSO API, there is a chance that the dataset that is loaded from SCHEMA_URL
        differs from the definition that is in de Postgresql database.
        """
        try:
            return self.datasets_cache[dataset_id]
        except KeyError:
            dataset = self._load_dataset(dataset_id)
            if dataset is None:
                raise ValueError(f"Dataset {dataset_id} is missing.") from None
            self.add_dataset(dataset)
