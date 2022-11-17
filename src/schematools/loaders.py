from __future__ import annotations

import contextlib
import json
import os
from functools import cached_property
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

import requests
from more_ds.network.url import URL

from schematools import DEFAULT_PROFILE_URL, DEFAULT_SCHEMA_URL
from schematools.exceptions import DatasetNotFound
from schematools.types import DatasetSchema, DatasetTableSchema, Json, ProfileSchema

__all__ = (
    "get_schema_loader",
    "CachedSchemaLoader",
    "FileSystemSchemaLoader",
    "URLSchemaLoader",
    "SchemaLoader",
)


class SchemaLoader:
    """Interface that defines what a schema loader should provide."""

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset for dataset_id."""
        raise NotImplementedError

    def get_table(self, dataset: DatasetSchema, table_ref: str) -> DatasetTableSchema:
        """Retrieves a versioned table by reference"""
        raise NotImplementedError

    def get_dataset_path(self, dataset_id: str) -> str:
        """Find the relative path of a dataset within the location"""
        raise NotImplementedError

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from the schema_url location.

        The return value maps dataset paths (foo/bar) to schema's.
        """
        raise NotImplementedError


class ProfileLoader:
    """Interface for loading profile objects"""

    def get_profile(self, profile_id: str) -> ProfileSchema:
        raise NotImplementedError()

    def get_all_profiles(self) -> list[ProfileSchema]:
        raise NotImplementedError()


class CachedSchemaLoader(SchemaLoader):
    """Base class for a loader that caches the results."""

    def __init__(self, loader: SchemaLoader | None):
        """Initialize the cache.
        When the loader is not defined, this acts as a simple cache.
        """
        self._loader = loader
        self._cache: dict[str, DatasetSchema] = {}
        self._table_cache: dict[tuple[str, str], DatasetTableSchema] = {}
        self._has_all = False

    def __repr__(self):
        return f"{self.__class__.__name__}({self._loader!r})"

    def add_dataset(self, dataset: DatasetSchema) -> None:
        """Add a dataset to the cache."""
        self._cache[dataset.id] = dataset

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()
        self._table_cache.clear()

    def get_dataset_path(self, dataset_id) -> str:
        # loader already caches that:
        if self._loader is None:
            raise RuntimeError("This dataset collection can't retrieve new datasets")
        return self._loader.get_dataset_path(dataset_id)

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset by id from the cache.

        If not available, load the dataset from the SCHEMA_URL location.
        NB. Because dataset schemas are imported into the Postgresql database
        by the DSO API, there is a chance that the dataset that is loaded from SCHEMA_URL
        differs from the definition that is in de Postgresql database.
        """
        try:
            return self._cache[dataset_id]
        except KeyError:
            if self._loader is None:
                raise RuntimeError("This dataset collection can't retrieve new datasets")
            dataset = self._loader.get_dataset(dataset_id, prefetch_related=prefetch_related)
            self.add_dataset(dataset)
            return dataset

    def get_table(self, dataset: DatasetSchema, table_ref: str) -> DatasetTableSchema:
        key = (dataset.id, table_ref)
        try:
            return self._table_cache[key]
        except KeyError:
            pass  # avoid raising exceptions from another exception

        if self._loader is None:
            raise RuntimeError("This dataset collection can't retrieve new datasets")

        table = self._loader.get_table(dataset, table_ref)
        self._table_cache[key] = table
        return table

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Load all datasets, and fill the cache"""
        if not self._has_all:
            if self._loader is None:
                raise RuntimeError("This dataset collection can't retrieve new datasets")

            self._cache = {
                schema.id: schema for schema in self._loader.get_all_datasets().values()
            }
            self._has_all = True

        return self._cache


class _FileBasedSchemaLoader(SchemaLoader):
    """Common logic for any schema loader that works with files (URLs or paths)"""

    def __init__(
        self, schema_url: URL | Path, *, loaded_callback: Callable[[DatasetSchema], None] = None
    ):
        # All the datasets loaded by this instance should be collected
        # into this single cached instance, so no duplicate instances are loaded.
        self.schema_url = schema_url
        self.dataset_collection = CachedSchemaLoader(self)
        self._loaded_callback = loaded_callback

    def __repr__(self):
        return f"{self.__class__.__name__}({self.schema_url!r})"

    @cached_property
    def _dataset_paths(self) -> dict[str, str]:
        """Cached index listing of all ID's to paths.
        This is typically needed for datasets that exist in sub folders,
        or when the folder name differs from the dataset ID.

        Since datasets can reference each other,
        this data is calculated for the whole repository at once.
        """
        return self._read_index()

    def _read_index(self) -> dict[str, str]:
        raise NotImplementedError

    def _read_dataset(self, dataset_id: str) -> Json:
        raise NotImplementedError

    def _read_table(self, dataset_id: str, table_ref: str) -> Json:
        raise NotImplementedError

    def get_dataset(self, dataset_id: str, prefetch_related: bool = False) -> DatasetSchema:
        """Gets a dataset from the filesystem for dataset_id."""
        schema_json = self._read_dataset(dataset_id)
        return self._as_dataset(schema_json)

    def _as_dataset(self, schema_json: dict, prefetch_related: bool = False) -> DatasetSchema:
        """Convert the read JSON into a real object that can resolve its relations."""
        dataset_schema = DatasetSchema(schema_json, dataset_collection=self.dataset_collection)

        if self._loaded_callback is not None:
            self._loaded_callback(dataset_schema)

        if prefetch_related:
            dataset_schema.tables  # noqa: ensure versioned tables are prefetched

            # Make sure the related datasets are read.
            for dataset_id in dataset_schema.related_dataset_schema_ids:
                self.dataset_collection.get_dataset(dataset_id, prefetch_related=True)

        return dataset_schema

    def get_dataset_path(self, dataset_id) -> str:
        """Find the relative path for a dataset."""
        try:
            # Since datasets are related, a cache is constructed.
            # Anything that is not part in this lookup table, will not be recognized as dataset.
            return self._dataset_paths[dataset_id]
        except KeyError:
            raise DatasetNotFound(
                f"Dataset '{dataset_id}' not found in '{self.schema_url}'."
            ) from None

    def get_table(self, dataset: DatasetSchema, table_ref: str) -> DatasetTableSchema:
        """Load a versioned table from the location."""
        try:
            table_json = self._read_table(dataset.id, table_ref)
        except DatasetNotFound:
            # Amend the error for a better understanding. In this case, the dataset was
            # likely using a non-standard naming format (loaded via get_dataset_from_file())
            # but now the path isn't resolvable through the index. Fix the loading
            raise RuntimeError(f"Can't determine path to dataset '{dataset}'!") from None
        return DatasetTableSchema(table_json, parent_schema=dataset)

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from the filesystem based on the `self.schema_url` path.
        Returns a dictionary of relative paths and their schema.
        """
        datasets = {}
        for dataset_id, dataset_path in sorted(self._dataset_paths.items()):
            dataset = self.get_dataset(dataset_id, prefetch_related=False)
            dataset.tables  # noqa: ensure versioned tables are still prefetched
            datasets[dataset_path] = dataset
        return datasets


class FileSystemSchemaLoader(_FileBasedSchemaLoader):
    """Loader that loads dataset schemas from the filesystem."""

    def __init__(
        self,
        schema_url: Path | str,
        *,
        loaded_callback: Callable[[DatasetSchema], None] | None = None,
    ):
        """Initialize the loader with a folder where it needs to search for datasets.
        For the convenience of importing a selected subset, it's possible
        to point to a subfolder of the datasets repository.
        """
        schema_url = Path(schema_url) if isinstance(schema_url, str) else schema_url
        if not schema_url.exists():
            raise FileNotFoundError(schema_url)
        if not schema_url.is_dir():
            raise ValueError(
                f"FileSystemSchemaLoader should receive a folder, not a file: '{schema_url}'."
            )

        super().__init__(schema_url, loaded_callback=loaded_callback)
        try:
            # For compatibility with importing subfolders, this loader allows to define
            # a subfolder as target. To avoid unexpected content in the datasets database,
            # still try to calculate the real root so any calculated dataset-paths will work.
            self.root = self.get_root(schema_url)
        except ValueError:
            # In case the real root can't be found (typically in unit tests with random layouts),
            # assume the given folder should be treated as the root folder.
            self.root = schema_url

        # All the datasets loaded by this instance will be collected
        # into a single cached version, so no duplicate instances are loaded.
        self._dataset_collection = CachedSchemaLoader(self)

    @classmethod
    def from_file(cls, dataset_file: Path | str, **kwargs):
        """Helper function to support old patterns of loading random files as schema."""
        root = cls.get_root(dataset_file)
        return cls(schema_url=root, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.schema_url!r})"

    @classmethod
    def get_root(cls, dataset_file: Path | str) -> Path:
        """Resolve the real root folder.
        This makes sure that the datasets-path value will be correct,
        even when a subfolder is selected for importing files.
        """
        dataset_file = Path(dataset_file)
        dataset_file.stat()  # confirms the file exists, raises FileNotFoundError otherwise.
        if dataset_file.name == "datasets":
            # Pointed to the datasets folder already.
            return dataset_file

        if (try_root := dataset_file.joinpath("datasets")).exists():
            # Pointed to the root folder that has the 'datasets' folder.
            return try_root

        # Check whether it's a specific dataset inside the repository.
        # Find repository root from the file
        try:
            return next(dir for dir in dataset_file.parents if dir.name == "datasets")
        except StopIteration:
            raise ValueError(f"No 'datasets' root found for file '{dataset_file}'.")

    def get_dataset_from_file(self, dataset_file: Path | str, prefetch_related: bool = False):
        """Extra method, to read a dataset directly from a JSON file.
        This is mainly a helper function for testing.

        Normally, datasets are only detected when they use the format ``folder/dataset.json``.
        This method allows a more free-format naming convention for experimenting with files
        (e.g. useful for unit testing). It will however not be possible to resolve relations
        to other datasets when those datasets use same free-format for naming their files.

        Concluding, any relations will only resolve when:

        * the dataset follows the ``name/dataset.json`` convention;
        * or when the related dataset is also read and cached by the same loader instance.
        """
        dataset_file = Path(dataset_file)
        if not dataset_file.is_absolute():
            dataset_file = self.root.joinpath(dataset_file)
        if not dataset_file.resolve().is_relative_to(self.root):
            raise ValueError(
                f"Dataset file '{dataset_file}' does not exist in the schema repository"
            )

        schema_json = _read_json_path(dataset_file)
        return self._as_dataset(schema_json, prefetch_related=prefetch_related)

    def _read_index(self) -> dict[str, str]:
        """A mapping of dataset ID to path."""
        # The index determines which datasets will be found.
        # For historical reasons, the filesystem loader can be initialized to work in a subfolder.
        # In that case, it will find fewer datasets, but still resolve them from the true root.
        id_to_path = {}
        for path in self.schema_url.glob("**/dataset.json"):
            file_json = _read_json_path(path)
            if not isinstance(file_json, dict) or file_json.get("type") != "dataset":
                continue

            id = file_json.get("id")
            if id in id_to_path:
                raise RuntimeError(
                    f"Schema root '{self.root}' contains multiple datasets that named '{id}', "
                    f"this will break relating datasets!"
                )
            id_to_path[id] = str(path.parent.relative_to(self.root))
        return id_to_path

    def _read_dataset(self, dataset_id):
        dataset_path = self.get_dataset_path(dataset_id)
        return _read_json_path(self.root / dataset_path / "dataset.json")

    def _read_table(self, dataset_id: str, table_ref: str) -> Json:
        dataset_path = self.get_dataset_path(dataset_id)
        return _read_json_path(self.root / dataset_path / f"{table_ref}.json")


def _read_json_path(dataset_file: Path) -> Json:
    """Load JSON from a path"""
    with dataset_file.open() as stream:
        try:
            return json.load(stream)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid Amsterdam Dataset schema file") from exc


class _SharedConnectionMixin:
    """Internal mixin for connection sharing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connection = None  # can be shared between methods

    @contextlib.contextmanager
    def _persistent_connection(self):
        """Context manager for having a single connection on retrieval."""
        if self._connection is not None:
            # Nested usage, just keep the connection.
            yield
        else:
            self._connection = requests.Session()
            with self._connection:  # calls .close() on exit
                yield
            self._connection = None

    def _read_json_url(self, url) -> Json:
        """Load JSON from an URL"""
        response = (self._connection or requests).get(url, timeout=60)
        response.raise_for_status()
        return response.json()


class URLSchemaLoader(_SharedConnectionMixin, _FileBasedSchemaLoader):
    """Loader that loads dataset schemas from an URL."""

    def __init__(
        self,
        schema_url: URL | str | None = None,
        *,
        loaded_callback: Callable[[DatasetSchema], None] | None = None,
    ):
        super().__init__(
            URL(schema_url or os.environ.get("SCHEMA_URL") or DEFAULT_SCHEMA_URL),
            loaded_callback=loaded_callback,
        )

    def get_all_datasets(self) -> dict[str, DatasetSchema]:
        """Gets all datasets from a web url based on the `self.schema_url` path."""
        with self._persistent_connection():
            return super().get_all_datasets()

    def get_dataset(self, dataset_id: str, prefetch_related: bool = True) -> DatasetSchema:
        """Retrieve a dataset and its contents with a single connection."""
        with self._persistent_connection():
            return super().get_dataset(dataset_id, prefetch_related=prefetch_related)

    def _read_index(self) -> dict[str, str]:
        return dict(self._read_json_url(self.schema_url / "index.json"))

    def _read_dataset(self, dataset_id: str) -> Json:
        dataset_path = self.get_dataset_path(dataset_id)
        return self._read_json_url(self.schema_url / dataset_path / "dataset")

    def _read_table(self, dataset_id: str, table_ref: str) -> Json:
        dataset_path = self.get_dataset_path(dataset_id)
        return self._read_json_url(self.schema_url / dataset_path / table_ref)


class FileSystemProfileLoader(ProfileLoader):
    """Loading profiles from the file system."""

    def __init__(
        self, profiles_url: Path | str, *, loaded_callback: Callable[[ProfileSchema], None] = None
    ):
        self.profiles_url = Path(profiles_url) if isinstance(profiles_url, str) else profiles_url
        self._loaded_callback = loaded_callback

    def get_profile(self, profile_id: str) -> ProfileSchema:
        """Load a specific profile by id."""
        data = _read_json_path(self.profiles_url / f"{profile_id}.json")
        schema = ProfileSchema.from_dict(data)
        if self._loaded_callback is not None:
            self._loaded_callback(schema)
        return schema

    def get_all_profiles(self) -> list[ProfileSchema]:
        """Load all profiles found in a folder"""
        return [
            ProfileSchema.from_dict(_read_json_path(path))
            for path in self.profiles_url.glob("**/*.json")
            if path.name != "index.json"
        ]


class URLProfileLoader(_SharedConnectionMixin, ProfileLoader):
    """Loading profiles from a URL"""

    def __init__(
        self,
        profiles_url: URL | str | None = None,
        *,
        loaded_callback: Callable[[ProfileSchema], None] = None,
    ):
        super().__init__()
        self.profiles_url = URL(
            profiles_url or os.environ.get("PROFILES_URL", DEFAULT_PROFILE_URL)
        )
        self._loaded_callback = loaded_callback

    def get_profile(self, profile_id: str) -> ProfileSchema:
        data = self._read_json_url(self.profiles_url / f"{profile_id}.json")
        schema = ProfileSchema.from_dict(data)
        self._loaded_callback(schema)
        return schema

    def get_all_profiles(self) -> list[ProfileSchema]:
        profiles = []
        with self._persistent_connection():
            index = self._read_json_url(self.profiles_url / "index.json")
            for name in index:
                profiles.append(self.get_profile(name))

        return profiles


def get_schema_loader(schema_url: URL | str | None = None, **kwargs) -> SchemaLoader:
    """Initialize the schema loader based on the given location.

    schema_url:
        Location where the schemas can be found. This
        can be a web url, or a filesystem path.
    """
    if schema_url is None:
        schema_url = os.environ.get("SCHEMA_URL") or DEFAULT_SCHEMA_URL

    if _is_url(schema_url):
        return URLSchemaLoader(schema_url, **kwargs)
    else:
        return FileSystemSchemaLoader(schema_url, **kwargs)


def get_profile_loader(profiles_url: URL | Path | str | None = None, **kwargs) -> ProfileLoader:
    """Initialize the profile loader for a given location."""
    if profiles_url is None:
        profiles_url = os.environ.get("PROFILES_URL") or DEFAULT_PROFILE_URL
    if _is_url(profiles_url):
        return URLProfileLoader(profiles_url, **kwargs)
    else:
        return FileSystemProfileLoader(profiles_url, **kwargs)


def _is_url(location: URL | str) -> bool:
    return isinstance(location, URL) or urlparse(location).scheme in ("http", "https")
