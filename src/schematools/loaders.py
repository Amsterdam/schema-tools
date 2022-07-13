from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from more_ds.network.url import URL

from schematools.exceptions import DatasetNotFound

if TYPE_CHECKING:
    from schematools.types import DatasetSchema


class SchemaLoader:
    """Base class for schema loaders."""

    def __init__(self, schema_url: Path | URL | str):
        """Initialize the schema loader.

        schema_url:
            Either a web url to the dataset schemas, or a path on the local filesystem.
        """
        self.schema_url: Path | URL = (
            schema_url if isinstance(schema_url, Path) else URL(schema_url)
        )

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset for dataset_id."""
        raise NotImplementedError

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from the schema_url location."""
        raise NotImplementedError


class FileSystemSchemaLoader(SchemaLoader):
    """Loader that loads dataset schemas from the filesystem."""

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset from the filesystem for dataset_id."""
        from schematools.utils import dataset_schema_from_id_and_schemas_path

        try:
            return dataset_schema_from_id_and_schemas_path(
                dataset_id,
                schemas_path=self.schema_url,
                prefetch_related=prefetch_related,
            )
        except FileNotFoundError as e:
            raise DatasetNotFound(f"Dataset `{dataset_id}` not found.") from e

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from the filesystem based on the `self.schema_url` path."""
        from schematools.utils import dataset_schemas_from_schemas_path

        return dataset_schemas_from_schemas_path(self.schema_url)


class URLSchemaLoader(SchemaLoader):
    """Loader that loads dataset schemas from a url."""

    def get_dataset(self, dataset_id: str, prefetch_related: bool = True) -> DatasetSchema:
        """Gets a dataset from a url for dataset_id."""
        from schematools.utils import dataset_schema_from_url

        try:
            return dataset_schema_from_url(
                self.schema_url, dataset_id, prefetch_related=prefetch_related
            )
        except KeyError:
            raise DatasetNotFound(f"Dataset `{dataset_id}` not found.") from None

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from a web url based on the `self.schema_url` path."""
        from schematools.utils import dataset_schemas_from_url

        return dataset_schemas_from_url(self.schema_url)  # type: ignore[no-any-return]
