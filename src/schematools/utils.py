from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Final, Match, Optional, Pattern, Type, Union, cast

import requests
from cachetools.func import lru_cache, ttl_cache
from string_utils import slugify

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR, TMP_TABLE_POSTFIX, types

RE_CAMEL_CASE: Final[Pattern[str]] = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z])"
)


@ttl_cache(ttl=16)
def schema_defs_from_url(
    schemas_url: str, dataset_name: Optional[str] = None
) -> Dict[str, types.DatasetSchema]:
    """Fetch all schema definitions from a remote file (or single dataset if specified).
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    if dataset_name:
        schema = def_from_url(
            base_url=schemas_url,
            data_type=types.DatasetSchema,
            dataset_id=dataset_name,
        )
        return {dataset_name: schema}

    return defs_from_url(base_url=schemas_url, data_type=types.DatasetSchema)


def schema_def_from_url(schemas_url: str, dataset_name: str) -> types.DatasetSchema:
    return def_from_url(
        base_url=schemas_url,
        data_type=types.DatasetSchema,
        dataset_id=dataset_name,
    )


@ttl_cache(ttl=16)
def profile_defs_from_url(profiles_url: str) -> Dict[str, types.ProfileSchema]:
    """Fetch all profile definitions from a remote file.
    The URL could be ``https://schemas.data.amsterdam.nl/profiles/``
    """
    return defs_from_url(base_url=profiles_url, data_type=types.ProfileSchema)


def defs_from_url(base_url: str, data_type: Type[types.ST]) -> Dict[str, types.ST]:
    """Fetch all schema definitions from a remote file.
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup: Dict[str, types.ST] = {}
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"

    with requests.Session() as connection:
        response = connection.get(f"{base_url}index.json")
        response.raise_for_status()
        response_data = response.json()

        for dataset_id, dataset_path in response_data.items():
            response = connection.get(f"{base_url}{dataset_path}")
            response.raise_for_status()
            response_data = response.json()
            response_data["url_prefix"] = get_dataset_prefix_from_path(
                dataset_path=dataset_path, dataset_data=response_data
            )

            schema_lookup[dataset_id] = data_type.from_dict(response_data)

    return schema_lookup


def def_from_url(base_url: str, data_type: Type[types.ST], dataset_id: str) -> types.ST:
    """Fetch schema definitions from a remote file for a single dataset
    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup: Dict[str, types.ST] = {}
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"

    with requests.Session() as connection:
        index_response = connection.get(f"{base_url}index.json")
        index_response.raise_for_status()
        index = index_response.json()

        response = connection.get(f"{base_url}{index[dataset_id]}")
        response.raise_for_status()

        schema_lookup[dataset_id] = data_type.from_dict(response.json())

    return schema_lookup[dataset_id]


def schema_def_from_file(filename: Union[Path, str]) -> Dict[str, types.DatasetSchema]:
    """Read schema definitions from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["id"]: types.DatasetSchema.from_dict(schema_info)}


def profile_def_from_file(filename: Union[Path, str]) -> Dict[str, types.DatasetSchema]:
    """Read a profile from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["name"]: types.DatasetSchema.from_dict(schema_info)}


def schema_fetch_url_file(schema_url_file: str) -> Dict[str, Any]:
    """Return schemadata from URL or File"""

    if not schema_url_file.startswith("http"):
        with open(schema_url_file) as f:
            schema_data = json.load(f)
    else:
        response = requests.get(schema_url_file)
        response.raise_for_status()
        schema_data = response.json()

    return cast(Dict[str, Any], schema_data)


_CAMEL_CASE_REPLACE_PAT: Final[Pattern[str]] = re.compile(
    r"""
    (?:_|\s)+   # Find word boundaries by looking for underscore and whitespace characters, they
                # will be discarded (not captured)
    (.)         # Capture first letter of word on word boundary
    |           # OR
    (\d+)       # Capture a number
    (?:_|\s)*   # Optionally followed by underscore and whitespace characters (to be discarded)
    (.)         # Capture first letter of word on word boundary
    """,
    re.VERBOSE,
)


@lru_cache(maxsize=500)
def toCamelCase(ident: str) -> str:
    """Convert an identifier to camelCase format.

    Word boundaries are determined by:
    - numbers
    - underscore characters
    - whitespace characters (this violates the concept of identifiers,
      but we handle it nevertheless)

    A camelCased identifier, when it starts with a letter, it will start with a lower cased letter.

    Empty strings are not allowed. They will raise an :exc:`ValueError` exception.

    Examples:

        >>> toCamelCase("dataset_table_schema")
        'datasetTableSchema'
        >>> toCamelCase("dataset table schema")
        'datasetTableSchema'
        >>> toCamelCase("fu_33_bar")
        'fu33Bar'
        >>> toCamelCase("fu_33bar")
        'fu33Bar'
        >>> toCamelCase("fu_33Bar")
        'fu33Bar'
        >>> toCamelCase("33_fu_bar")
        '33FuBar'

    Args:
        ident: The identifier to be converted.

    Returns:
        The identifier in camelCase format.

    Raises:
        ValueError: If ``indent`` is an empty string.

    """

    def replacement(m: Match) -> str:
        # As we use the OR operator in the regular expression with capture groups on both sides,
        # we will always have at least one capture group that results in `None`. We filter those
        # out in the generator expression. Even though a captured group sometimes represents a
        # number (as a string), we still call `upper()` on it. That's faster than another
        # explicit test.
        return "".join(s.upper() for s in m.groups() if s)

    if ident == "":
        raise ValueError("Parameter `ident` cannot be an empty string.")
    result = _CAMEL_CASE_REPLACE_PAT.sub(replacement, ident)
    # The first letter of camelCase identifier is always lower case
    return result[0].lower() + result[1:]


@lru_cache(maxsize=500)
def to_snake_case(ident: str) -> str:
    """Convert an identifier to snake_case format.

    Empty strings are not allowed. They will raise an :exc:`ValueError` exception.

    Args:
        ident: The identifier to be converted.

    Returns:
        The identifier in snake_case foramt.

    Raises:
        ValueError: If ``ident`` is an empty string.
    """
    if ident == "":
        raise ValueError("Parameter `ident` cannot be an empty string.")
    # Convert to field name, avoiding snake_case to snake_case issues.
    # Also preserve RELATION_INDICATOR in names (RELATION_INDICATOR are used for object relations)
    name_parts = [toCamelCase(part) for part in ident.split(RELATION_INDICATOR)]
    return RELATION_INDICATOR.join(
        slugify(RE_CAMEL_CASE.sub(r" \1", part).strip(), separator="_") for part in name_parts
    )


def get_rel_table_identifier(
    prefix_length: int, table_identifier: str, through_identifier: str
) -> str:
    """Create identifier for related table (FK or M2M) from table_identifier and an extra fieldname.
    Take length of prefix (dataset.id) into account, postgresql has maxsize for tablenames."""
    through_table_name = f"{table_identifier}_{through_identifier}"
    return through_table_name
    # return through_table_name[: MAX_TABLE_NAME_LENGTH - len(TMP_TABLE_POSTFIX) - prefix_length]


def shorten_name(db_table_name: str, with_postfix: bool = False) -> str:
    """ Utility function to shorten names to safe length for postgresql """
    max_length = MAX_TABLE_NAME_LENGTH - int(with_postfix) * len(TMP_TABLE_POSTFIX)
    return db_table_name[:max_length]


def get_dataset_prefix_from_path(dataset_path: str, dataset_data: dict) -> str:
    """
    Extract dataset prefix from dataset path.
    """
    version = dataset_data.get("version")
    if version:
        dataset_path = dataset_path.split(version)[0]

    dataset_parts = dataset_path.split("/")[:-1]
    if to_snake_case(dataset_parts[-1]) == to_snake_case(dataset_data["id"]):
        dataset_parts.pop()
    return "/".join(dataset_parts)
