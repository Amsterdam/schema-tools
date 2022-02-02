from __future__ import annotations

import os
from typing import TYPE_CHECKING, Dict, Optional, Union
from urllib.parse import urlparse

from more_ds.network.url import URL
from simple_singleton import Singleton

if TYPE_CHECKING:
    from schematools.types import DatasetSchema

from schematools import DEFAULT_SCHEMA_URL, loaders

# Initialize the schema_loader in the DatasetCollection singleton


class DatasetCollection(metaclass=Singleton):
    """Holding class for a collection of datasets.

    This can hold a cache of datasets that have already been collected.
    This class is a singleton class, so every DatasetSchema can have
    a reference to it, without creating redundancy.
    """

    def __init__(self) -> None:  # noqa: D107
        self.datasets_cache: Dict[str, DatasetSchema] = {}
        self._schema_loader: Optional[loaders.SchemaLoader] = None

    @property
    def schema_loader(self) -> loaders.SchemaLoader:
        """Get the schema_loader."""
        if self._schema_loader is None:
            raise ValueError("The datasetcollection should be initialized with a schema loader")
        return self._schema_loader

    @schema_loader.setter
    def schema_loader(self, schema_loader: loaders.SchemaLoader) -> None:
        """Set the schema loader for the datasetcollection."""
        self._schema_loader = schema_loader

    def _load_dataset(self, dataset_id: str, prefetch_related: bool) -> DatasetSchema:
        """Loads the dataset, using the configured loader."""
        return self.schema_loader.get_dataset(dataset_id, prefetch_related=prefetch_related)

    def add_dataset(self, dataset: DatasetSchema) -> None:
        """Add a dataset to the cache."""
        self.datasets_cache[dataset.id] = dataset

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset by id from the cache.

        If not available, load the dataset from the SCHEMA_URL location.
        NB. Because dataset schemas are imported into the Postgresql database
        by the DSO API, there is a chance that the dataset that is loaded from SCHEMA_URL
        differs from the definition that is in de Postgresql database.
        """
        try:
            return self.datasets_cache[dataset_id]
        except KeyError:
            dataset = self._load_dataset(dataset_id, prefetch_related=prefetch_related)
            self.add_dataset(dataset)
            return dataset

    def get_all_datasets(self) -> Dict[str, DatasetSchema]:
        """Gets all the datasets using the configured schema loader."""
        return self.schema_loader.get_all_datasets()


def set_schema_loader(schema_url: Union[URL, str]) -> None:
    """Initialize the schema loader at module load time.

    schema_url:
        Location where the schemas can be found. This
        can be a web url, or a filesystem path.
    """
    dataset_collection = DatasetCollection()
    loader: Optional[loaders.SchemaLoader] = None  # pleasing mypy
    if urlparse(schema_url).scheme in ("http", "https"):
        loader = loaders.URLSchemaLoader(schema_url)
    else:
        loader = loaders.FileSystemSchemaLoader(schema_url)
    dataset_collection.schema_loader = loader


# The scheme loader is initialized from the `SCHEMA_URL` environment variable,
# or from the DEFAULT_SCHEMA_URL constant.
# This call is done at module load time, to have an initial value for the schemaloader.
# If needed, an alternative schemaloader can be injected into `DatasetCollection` at runtime.
set_schema_loader(os.environ.get("SCHEMA_URL", DEFAULT_SCHEMA_URL))
