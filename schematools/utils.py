from __future__ import annotations

import json
import re
from typing import Dict

from cachetools.func import ttl_cache, lru_cache
import requests
from string_utils import slugify

from . import types
from . import RELATION_INDICATOR


re_camel_case = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z])"
)


@ttl_cache(ttl=16)  # type: ignore
def schema_defs_from_url(schemas_url, dataset_name=None) -> Dict[str, types.DatasetSchema]:
    """Fetch all schema definitions from a remote file (or single dataset if specified).
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    if dataset_name:
        return def_from_url(
            base_url=schemas_url,
            data_type=types.DatasetSchema,
            dataset_name=dataset_name,
        )

    return defs_from_url(base_url=schemas_url, data_type=types.DatasetSchema)


def schema_def_from_url(schemas_url, schema_name):
    schemas = schema_defs_from_url(schemas_url)
    try:
        return schemas[schema_name]
    except KeyError:
        avail = ", ".join(sorted(schemas.keys()))
        raise ValueError(
            f"Schema f{schema_name} does not exist at {schemas_url}. Available are: {avail}"
        ) from None


@ttl_cache(ttl=16)  # type: ignore
def profile_defs_from_url(profiles_url) -> Dict[str, types.ProfileSchema]:
    """Fetch all profile definitions from a remote file.
    The URL could be ``https://schemas.data.amsterdam.nl/profiles/``
    """
    return defs_from_url(base_url=profiles_url, data_type=types.ProfileSchema)


def defs_from_url(base_url, data_type):
    """Fetch all schema definitions from a remote file.
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup = {}
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"

    with requests.Session() as connection:
        response = connection.get(f"{base_url}index.json")
        response.raise_for_status()
        response_data = response.json()

        for dataset_name, dataset_path in response_data.items():
            response = connection.get(f"{base_url}{dataset_path}")
            response.raise_for_status()
            response_data = response.json()

            schema_lookup[dataset_name] = data_type.from_dict(response.json())

    return schema_lookup


def def_from_url(base_url, data_type, dataset_name):
    """Fetch schema definitions from a remote file for a single dataset
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup = {}
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"

    dataset_path = f"{dataset_name}/{dataset_name}"

    with requests.Session() as connection:
        response = connection.get(f"{base_url}{dataset_path}")
        response.raise_for_status()

        schema_lookup[dataset_name] = data_type.from_dict(response.json())

    return schema_lookup[dataset_name]


def schema_def_from_file(filename) -> Dict[str, types.DatasetSchema]:
    """Read schema definitions from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["id"]: types.DatasetSchema.from_dict(schema_info)}


def profile_def_from_file(filename) -> Dict[str, types.DatasetSchema]:
    """Read a profile from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["name"]: types.DatasetSchema.from_dict(schema_info)}


def schema_fetch_url_file(schema_url_file):
    """Return schemadata from URL or File"""

    if not schema_url_file.startswith("http"):
        with open(schema_url_file) as f:
            schema_location = json.load(f)
    else:
        response = requests.get(schema_url_file)
        response.raise_for_status()
        schema_location = response.json()

    return schema_location


def toCamelCase(name):
    """
    Unify field/column/dataset name from Space separated/Snake Case/Camel case
    to camelCase.
    """
    name = " ".join(name.split("_"))
    words = re_camel_case.sub(r" \1", name).strip().lower().split(" ")
    return "".join(w.lower() if i == 0 else w.title() for i, w in enumerate(words))


@lru_cache(maxsize=256)  # type: ignore
def to_snake_case(name):
    """
    Convert field/column/dataset name from Space separated/Snake Case/Camel case
    to snake_case.
    """
    # Convert to field name, avoiding snake_case to snake_case issues.
    # Also preserve RELATION_INDICATOR in names (RELATION_INDICATOR are used for object relations)
    name_parts = [toCamelCase(part) for part in name.split(RELATION_INDICATOR)]
    return RELATION_INDICATOR.join(
        slugify(re_camel_case.sub(r" \1", part).strip().lower(), separator="_")
        for part in name_parts
    )
