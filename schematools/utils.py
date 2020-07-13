from __future__ import annotations

import json
import re
from typing import Dict

from cachetools.func import ttl_cache
import requests
from string_utils import slugify

from . import types


re_camel_case = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z])"
)


@ttl_cache(ttl=16)
def schema_defs_from_url(schemas_url) -> Dict[str, types.DatasetSchema]:
    """Fetch all schema definitions from a remote file.
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup = {}
    if not schemas_url.endswith("/"):
        schemas_url = f"{schemas_url}/"

    with requests.Session() as connection:
        response = connection.get(f"{schemas_url}index.json")
        response.raise_for_status()
        response_data = response.json()

        for dataset_name, dataset_path in response_data.items():
            response = connection.get(f"{schemas_url}{dataset_path}")
            response.raise_for_status()
            response_data = response.json()

            schema_lookup[dataset_name] = types.DatasetSchema.from_dict(response.json())

    return schema_lookup


def schema_def_from_url(schemas_url, schema_name):
    schemas = schema_defs_from_url(schemas_url)
    try:
        return schemas[schema_name]
    except KeyError:
        avail = ", ".join(sorted(schemas.keys()))
        raise ValueError(
            f"Schema f{schema_name} does not exist at {schemas_url}. Available are: {avail}"
        )


def schema_def_from_file(filename) -> Dict[str, types.DatasetSchema]:
    """Read schema definitions from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["id"]: types.DatasetSchema.from_dict(schema_info)}


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


def to_snake_case(name):
    """
    Convert field/column/dataset name from Space separated/Snake Case/Camel case
    to snake_case.
    """
    # Convert to field name, avoiding snake_case to snake_case issues.
    name = toCamelCase(name)
    return slugify(re_camel_case.sub(r" \1", name).strip().lower(), separator="_")
