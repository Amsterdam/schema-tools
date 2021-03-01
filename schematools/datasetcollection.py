from __future__ import annotations

from typing import TYPE_CHECKING

from simple_singleton import Singleton

if TYPE_CHECKING:
    from schematools.types import DatasetSchema


class DatasetCollection(metaclass=Singleton):
    """Holding class for a collection of datasets.
    This can hold a cache of datasets that have already been collected.
    """

    def __init__(self) -> None:
        self.datasets_cache = {}

    def add_dataset(self, dataset: DatasetSchema) -> None:
        self.datasets_cache[dataset.id] = dataset

    def get_dataset(self, dataset_id: str) -> DatasetSchema:
        """ Gets a dataset by id, if not available, load the dataset """
        try:
            return self.datasets_cache[dataset_id]
        except KeyError:
            # Get the dataset from the SCHEMA_DEFS_URL
            # Only things is that it can differ from the schema that is
            # in the django db model.
            raise ValueError(f"Dataset {dataset_id} is missing.") from None
