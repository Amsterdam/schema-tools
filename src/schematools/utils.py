from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests
from cachetools.func import ttl_cache
from more_ds.network.url import URL

from schematools import types

logger = logging.getLogger(__name__)


@ttl_cache(ttl=16)  # type: ignore[misc]
def dataset_schemas_from_url(
    schemas_url: URL | str,
    dataset_name: str | None = None,
    prefetch_related: bool = False,
) -> dict[str, types.DatasetSchema]:
    """Fetch all dataset schemas from a remote file (or single dataset if specified).

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    if dataset_name:
        schema = schema_from_url(
            base_url=schemas_url,
            data_type=types.DatasetSchema,
            dataset_id=dataset_name,
            prefetch_related=prefetch_related,
        )
        return {dataset_name: schema}

    return schemas_from_url(base_url=schemas_url, data_type=types.DatasetSchema)


def dataset_schema_from_url(
    schemas_url: URL | str,
    dataset_name: str,
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Fetch a dataset schema from a remote file."""
    return schema_from_url(
        base_url=schemas_url,
        data_type=types.DatasetSchema,
        dataset_id=dataset_name,
        prefetch_related=prefetch_related,
    )


def schemas_from_url(base_url: URL | str, data_type: type[types.ST]) -> dict[str, types.ST]:
    """Fetch all schema definitions from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schemas = {}
    base_url = URL(base_url)

    with requests.Session() as connection:
        response = connection.get(base_url / "index.json")
        response.raise_for_status()
        response_data = response.json()

        for i, (schema_id, schema_path) in enumerate(response_data.items()):
            logger.debug("Looking up dataset %3d of %d: %s.", i, len(response_data), schema_id)
            schemas[schema_path] = _schema_from_url_with_connection(
                connection, base_url, schema_path, data_type
            )
    return schemas


def schema_from_url(
    base_url: URL | str,
    data_type: type[types.ST],
    dataset_id: str,
    prefetch_related: bool = False,
) -> types.ST:
    """Fetch schema definitions from a remote file for a single dataset.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    base_url = URL(base_url)

    with requests.Session() as connection:
        index_response = connection.get(base_url / "index.json")
        index_response.raise_for_status()
        index = index_response.json()
        dataset_schema = _schema_from_url_with_connection(
            connection, base_url, index[dataset_id], data_type
        )

    # For this recursive call, we set prefetch_related=False
    # to avoid deep/endless recursion
    # The result of def_from_url does not need to be stored,
    # because is it cached on the DatasetSchema instances.
    if prefetch_related and isinstance(dataset_schema, types.DatasetSchema):
        for ds_id in dataset_schema.related_dataset_schema_ids:
            schema_from_url(base_url, data_type, ds_id, prefetch_related=False)

    return dataset_schema


def _schema_from_url_with_connection(
    connection: requests.Session,
    base_url: URL,
    dataset_path: str,
    data_type: type[types.ST],
) -> types.ST:
    """Fetch single schema from url with connection."""
    response = connection.get(base_url / dataset_path / "dataset")
    response.raise_for_status()
    response_data = response.json()

    from schematools.types import SemVer, TableVersions

    # Include referenced tables for datasets.
    for i, table in enumerate(response_data["tables"]):
        if ref := table.get("$ref"):
            table_response = connection.get(base_url / dataset_path / ref)
            table_response.raise_for_status()
            # Assume `ref` is of form "table_name/v1.1.0"
            dvn = SemVer(ref.split("/")[-1])
            response_data["tables"][i] = TableVersions(
                id=table["id"], default_version_number=dvn, active={dvn: table_response.json()}
            )
            for version, ref in table.get("activeVersions", {}).items():
                table_response = connection.get(base_url / dataset_path / ref)
                table_response.raise_for_status()
                response_data["tables"][i].active[SemVer(version)] = table_response.json()
        else:
            dvn = SemVer(table["version"])
            response_data["tables"][i] = TableVersions(
                id=table["id"], default_version_number=dvn, active={dvn: table}
            )

    schema: types.ST = data_type.from_dict(response_data)
    return schema


def dataset_schema_from_path(
    dataset_path: Path | str,
) -> types.DatasetSchema:
    """Read a dataset schema from the filesystem.

    Args:
        dataset_path: Filesystem path to the dataset.
    """
    with open(dataset_path) as fh:
        try:
            ds = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid Amsterdam Dataset schema file") from exc

        if ds["type"] == "dataset":
            from schematools.types import SemVer, TableVersions

            for i, table in enumerate(ds["tables"]):
                if ref := table.get("$ref"):
                    # Assume `ref` is of form "table_name/v1.1.0"
                    dvn = SemVer(ref.split("/")[-1])
                    with open(Path(dataset_path).parent / Path(ref + ".json")) as table_file:
                        ds["tables"][i] = TableVersions(
                            id=table["id"],
                            default_version_number=dvn,
                            active={dvn: json.load(table_file)},
                        )
                    for version, ref in table.get("activeVersions", {}).items():
                        with open(Path(dataset_path).parent / Path(ref + ".json")) as table_file:
                            ds["tables"][i].active[SemVer(version)] = json.load(table_file)
                else:
                    dvn = SemVer(table["version"])
                    ds["tables"][i] = TableVersions(
                        id=table["id"], default_version_number=dvn, active={dvn: table}
                    )
    return types.DatasetSchema.from_dict(ds)


def _get_relative_dataset_path(schemas_path: Path | str, dataset_id: str) -> Path:
    """Gets the path of a dataset relative to `schemas_path`."""
    for root, dirs, files in os.walk(schemas_path):
        if dataset_id in dirs and "dataset.json" in files:
            return dataset_id
        for file_name in files:
            if file_name == "dataset.json":
                with (Path(root) / file_name).open() as jf:
                    dataset_json = json.load(jf)
                if dataset_json.get("id") == dataset_id:
                    return Path(root).relative_to(schemas_path)
    raise ValueError(f"No local path for dataset `{dataset_id}` found at `{schemas_path}`.")


def dataset_schema_from_id_and_schemas_path(
    dataset_id: str,
    schemas_path: Path | str,
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Read a dataset schema from a file on local drive.

    Args:
        dataset_id: Id of the dataset.
        schemas_path: Path to the location with the dataset schemas.
        prefetch_related: If True, the related datasets are preloaded.
    """
    relative_dataset_path = _get_relative_dataset_path(schemas_path, dataset_id)
    dataset_path = Path(schemas_path) / relative_dataset_path / "dataset.json"
    dataset_schema: types.DatasetSchema = dataset_schema_from_path(dataset_path)

    index: dict[str, Path] = {}

    # Build the mapping from dataset -> path with jsonschema file
    if prefetch_related:
        for root, _, files in os.walk(schemas_path):
            if "dataset.json" in files:
                root_path = Path(root)
                index[root_path.name] = root_path.joinpath("dataset.json")

        # Result of `dataset_schema_from_path` is discarded here, the call is only done
        # to add the dataset to the cache.
        for ds_id in dataset_schema.related_dataset_schema_ids:
            dataset_schema_from_path(index[ds_id])

    return dataset_schema


def dataset_schemas_from_schemas_path(root: Path | str) -> dict[str, types.DatasetSchema]:
    """Read all dataset schemas from a directory tree.

    Args:
        root: Path of a directory containing dataset schemas in subdirs.
    """
    schemas = {}
    for dirname, _, files in os.walk(root):
        if "dataset.json" in files:
            schema_path = os.path.join(dirname, "dataset.json")
            schema: types.DatasetSchema = dataset_schema_from_path(schema_path)
            rel_path = os.path.relpath(dirname, start=root)
            schemas[rel_path] = schema
    return schemas


def profile_schema_from_file(filename: Path | str) -> dict[str, types.ProfileSchema]:
    """Read a profile schema from a file on local drive."""
    with open(filename) as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["name"]: types.ProfileSchema.from_dict(schema_info)}
