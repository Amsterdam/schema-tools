from __future__ import annotations

import logging
import warnings
from pathlib import Path

from more_ds.network.url import URL

from schematools import types
from schematools.loaders import FileSystemSchemaLoader, URLSchemaLoader

logger = logging.getLogger(__name__)
_shared_fs_loader = None

warnings.warn(
    "Using schematools.utils is deprecated, use the `schematools.loaders` module instead.",
    DeprecationWarning,
)


def dataset_schemas_from_url(
    schemas_url: URL | str,
    dataset_name: str | None = None,
    prefetch_related: bool = False,
) -> dict[str, types.DatasetSchema]:
    warnings.warn(
        "Using dataset_schema_from_url() is deprecated, "
        "use `schematools.loaders.URLSchemaLoader` or `get_schema_loader()` instead.",
        DeprecationWarning,
    )

    loader = URLSchemaLoader(URL(schemas_url))
    if dataset_name:
        return {
            loader.get_dataset_path(dataset_name): loader.get_dataset(
                dataset_name, prefetch_related=prefetch_related
            )
        }
    else:
        return loader.get_all_datasets()


def dataset_schema_from_url(
    schemas_url: URL | str,
    dataset_name: str,
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Fetch a dataset schema from a remote file."""
    warnings.warn(
        "Using dataset_schema_from_url() is deprecated, "
        "use `schematools.loaders.URLSchemaLoader` or `get_schema_loader()` instead.",
        DeprecationWarning,
    )

    loader = URLSchemaLoader(schemas_url)
    return loader.get_dataset(dataset_name, prefetch_related=prefetch_related)


def schemas_from_url(base_url: URL | str, data_type: type) -> dict[str, types.DatasetSchema]:
    """Fetch all schema definitions from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    warnings.warn(
        "Using schemas_from_url() is deprecated, "
        "use `schematools.loaders.URLSchemaLoader().get_all_datasets()` instead.",
        DeprecationWarning,
    )
    if data_type is not types.DatasetSchema:
        raise TypeError("schemas_from_url() only worked with DatasetSchema")

    loader = URLSchemaLoader(base_url)
    return loader.get_all_datasets()


def schema_from_url(
    base_url: URL | str,
    data_type: type,
    dataset_id: str,
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Fetch schema definitions from a remote file for a single dataset.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    warnings.warn(
        "Using schema_from_url() is deprecated, "
        "use `schematools.loaders.URLSchemaLoader().get_dataset()` instead.",
        DeprecationWarning,
    )
    if data_type is not types.DatasetSchema:
        raise TypeError("schema_from_url() only worked with DatasetSchema")

    loader = URLSchemaLoader(base_url)
    return loader.get_dataset(dataset_id, prefetch_related=prefetch_related)


def dataset_schema_from_path(dataset_path: Path | str) -> types.DatasetSchema:
    """Read a dataset schema from the filesystem.

    Args:
        dataset_path: Filesystem path to the dataset.
    """
    warnings.warn(
        "Using dataset_schema_from_path() is deprecated, "
        "use `schematools.loaders.FileSystemSchemaLoader` instead.",
        DeprecationWarning,
    )

    # The filesystem loader is stored in a global instance,
    # so all loaded datasets are part of the same "dataset collection".
    global _shared_fs_loader
    if _shared_fs_loader is None:
        try:
            root = FileSystemSchemaLoader.get_root(dataset_path)
        except ValueError:
            root = dataset_path.parent
        _shared_fs_loader = FileSystemSchemaLoader(root)

    return _shared_fs_loader.get_dataset_from_file(dataset_path)


def dataset_schemas_from_schemas_path(root: Path | str) -> dict[str, types.DatasetSchema]:
    """Read all dataset schemas from a directory tree.

    Args:
        root: Path of a directory containing dataset schemas in subdirs.
    """
    warnings.warn(
        "Using dataset_schema_from_path() is deprecated, "
        "use `schematools.loaders.FileSystemSchemaLoader.get_all_datasets()` instead.",
        DeprecationWarning,
    )

    loader = FileSystemSchemaLoader(root)
    return loader.get_all_datasets()


def publishers_from_url(base_url: URL | str) -> dict[str, list[types.Publisher]]:
    """
    The URL could be ``https://schemas.data.amsterdam.nl/publishers/``
    """

    loader = URLSchemaLoader(base_url)
    return loader.get_all_publishers()
