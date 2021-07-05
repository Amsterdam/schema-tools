from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Final, Match, Optional, Pattern, Type, Union, cast

import requests
from cachetools.func import ttl_cache
from more_ds.network.url import URL
from string_utils import slugify

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR, TMP_TABLE_POSTFIX, types

RE_CAMEL_CASE: Final[Pattern[str]] = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z])"
)


@ttl_cache(ttl=16)
def schema_defs_from_url(
    schemas_url: Union[URL, str],
    dataset_name: Optional[str] = None,
    prefetch_related: bool = False,
) -> Dict[str, types.DatasetSchema]:
    """Fetch all schema definitions from a remote file (or single dataset if specified).

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    if dataset_name:
        schema = def_from_url(
            base_url=schemas_url,
            data_type=types.DatasetSchema,
            dataset_id=dataset_name,
            prefetch_related=prefetch_related,
        )
        return {dataset_name: schema}

    return defs_from_url(base_url=schemas_url, data_type=types.DatasetSchema)


def schema_def_from_url(
    schemas_url: Union[URL, str],
    dataset_name: str,
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Fetch schema definition from a remote file."""
    return def_from_url(
        base_url=schemas_url,
        data_type=types.DatasetSchema,
        dataset_id=dataset_name,
        prefetch_related=prefetch_related,
    )


@ttl_cache(ttl=16)
def profile_defs_from_url(profiles_url: Union[URL, str]) -> Dict[str, types.ProfileSchema]:
    """Fetch all profile definitions from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/profiles/``
    """
    return defs_from_url(base_url=profiles_url, data_type=types.ProfileSchema)


def defs_from_url(base_url: Union[URL, str], data_type: Type[types.ST]) -> Dict[str, types.ST]:
    """Fetch all schema definitions from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup: Dict[str, types.ST] = {}
    base_url = URL(base_url)

    with requests.Session() as connection:
        response = connection.get(base_url / "index.json")
        response.raise_for_status()
        response_data = response.json()

        for dataset_id, dataset_path in response_data.items():
            schema_lookup[dataset_id] = _def_from_url_with_connection(
                connection, base_url, dataset_path, data_type
            )
    return schema_lookup


def def_from_url(
    base_url: Union[URL, str],
    data_type: Type[types.ST],
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
        dataset_schema = _def_from_url_with_connection(
            connection, base_url, index[dataset_id], data_type
        )

    # For this recursive call, we set prefetch_related=False
    # to avoid deep/endless recursion
    # The result of def_from_url does not need to be stored,
    # because is it cached on the DatasetSchema instances.
    if prefetch_related and isinstance(dataset_schema, types.DatasetSchema):
        for ds_id in dataset_schema.related_dataset_schema_ids:
            def_from_url(base_url, data_type, ds_id, prefetch_related=False)

    return dataset_schema


def _def_from_url_with_connection(
    connection: requests.Session, base_url: URL, dataset_path: str, data_type: Type[types.ST]
) -> types.ST:
    """Fetch single dataset from url with connection."""
    response = connection.get(base_url / dataset_path / "dataset")
    response.raise_for_status()
    response_data = response.json()
    response_data["path"] = dataset_path

    # Include referenced tables for datasets.
    for i, table in enumerate(response_data["tables"]):
        if ref := table.get("$ref"):
            table_response = connection.get(base_url / dataset_path / ref)
            table_response.raise_for_status()
            response_data["tables"][i] = table_response.json()

    dataset_schema: types.ST = data_type.from_dict(response_data)
    return dataset_schema


def schema_def_from_file(filename: Union[Path, str]) -> Dict[str, types.DatasetSchema]:
    """Read schema definitions from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["id"]: types.DatasetSchema.from_dict(schema_info)}


def profile_def_from_file(filename: Union[Path, str]) -> Dict[str, types.ProfileSchema]:
    """Read a profile from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["name"]: types.ProfileSchema.from_dict(schema_info)}


def schema_fetch_url_file(schema_url_file: Union[URL, str]) -> Dict[str, Any]:
    """Return schemadata from URL or File."""
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

    Examples::

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
    """Create identifier for related table (FK or M2M) from table_identifier and extra fieldname.

    Take length of prefix (dataset.id) into account, postgresql has maxsize for tablenames.
    """
    through_table_name = f"{table_identifier}_{through_identifier}"
    return through_table_name

    # return through_table_name[: MAX_TABLE_NAME_LENGTH - len(TMP_TABLE_POSTFIX) - prefix_length]


def shorten_name(db_table_name: str, with_postfix: bool = False) -> str:
    """Shorten names to safe length for postgresql."""
    max_length = MAX_TABLE_NAME_LENGTH - int(with_postfix) * len(TMP_TABLE_POSTFIX)
    return db_table_name[:max_length]
