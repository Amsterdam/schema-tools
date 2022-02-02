from __future__ import annotations

import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Final, Match, Optional, Pattern, Type, Union, cast

import requests
from cachetools.func import ttl_cache
from deprecated import deprecated
from more_ds.network.url import URL
from more_itertools import last
from string_utils import slugify

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR, TMP_TABLE_POSTFIX, types

if TYPE_CHECKING:
    from schematools.loaders import SchemaLoader  # noqa: F401

RE_CAMEL_CASE: Final[Pattern[str]] = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z])"
)

logger = logging.getLogger(__name__)


@ttl_cache(ttl=16)  # type: ignore[misc]
def dataset_schemas_from_url(
    schemas_url: Union[URL, str],
    dataset_name: Optional[str] = None,
    prefetch_related: bool = False,
) -> Dict[str, types.DatasetSchema]:
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
    schemas_url: Union[URL, str],
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


def profile_schemas_from_url(profiles_url: Union[URL, str]) -> Dict[str, types.ProfileSchema]:
    """Fetch all profile schemas from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/profiles/``
    """
    return schemas_from_url(base_url=profiles_url, data_type=types.ProfileSchema)


def dataset_paths_from_url(base_url: Union[URL, str]) -> Dict[str, str]:
    """Fetch all dataset paths from a remote location.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    base_url = URL(base_url)

    with requests.Session() as connection:
        response = connection.get(base_url / "index.json")
        response.raise_for_status()
        return cast(Dict[str, str], response.json())


def schemas_from_url(base_url: Union[URL, str], data_type: Type[types.ST]) -> Dict[str, types.ST]:
    """Fetch all schema definitions from a remote file.

    The URL could be ``https://schemas.data.amsterdam.nl/datasets/``
    """
    schema_lookup: Dict[str, types.ST] = {}
    base_url = URL(base_url)

    with requests.Session() as connection:
        response = connection.get(base_url / "index.json")
        response.raise_for_status()
        response_data = response.json()

        for i, schema_id in enumerate(response_data):
            schema_path = response_data[schema_id]
            logger.debug("Looking up dataset %3d of %d: %s.", i, len(response_data), schema_id)
            schema_lookup[schema_id] = _schema_from_url_with_connection(
                connection, base_url, schema_path, data_type
            )
    return schema_lookup


def schema_from_url(
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
    data_type: Type[types.ST],
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
            dvn = SemVer(last(ref.split("/")))
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


@deprecated(
    version="2.3.1",
    reason="""The `dataset_schema_from_file` is replaced by `dataset_schema_from_path`.""",
)
def dataset_schema_from_file(
    file_path: Union[Path, str], prefetch_related: bool = False
) -> types.DatasetSchema:
    """Gets a dataset scheme from a file path."""
    return dataset_schema_from_path(file_path)


def dataset_schema_from_path(
    dataset_path: Union[Path, str],
) -> types.DatasetSchema:
    """Read a dataset schema from the filesystem.

    Args:
        dataset_path: Filesystem path to the dataset.
    """
    with open(dataset_path) as fh:
        try:
            ds = json.load(fh)
        except Exception as exc:
            raise ValueError("Invalid Amsterdam Dataset schema file") from exc

        if ds["type"] == "dataset":
            from schematools.types import SemVer, TableVersions

            for i, table in enumerate(ds["tables"]):
                if ref := table.get("$ref"):
                    # Assume `ref` is of form "table_name/v1.1.0"
                    dvn = SemVer(last(ref.split("/")))
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


def dataset_schema_from_id_and_schemas_path(
    dataset_id: str,
    schemas_path: Union[Path, str],
    prefetch_related: bool = False,
) -> types.DatasetSchema:
    """Read a dataset schema from a file on local drive.

    Args:
        dataset_id: Id of the dataset.
        schemas_path: Path to the location with the dataset schemas.
        prefetch_related: If True, the related datasets are preloaded.
    """
    dataset_path = Path(schemas_path) / dataset_id / "dataset.json"
    dataset_schema: types.DatasetSchema = dataset_schema_from_path(dataset_path)

    index: Dict[str, Path] = {}

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


def dataset_schemas_from_schemas_path(
    schemas_path: Union[Path, str]
) -> Dict[str, types.DatasetSchema]:
    """Read all datasets from the schemas_path.

    Args:
        schemas_path: Path to the filesystem location with the dataset schemas.
    """
    schema_lookup: Dict[str, types.DatasetSchema] = {}
    for root, _, files in os.walk(schemas_path):
        if "dataset.json" in files:
            root_path = Path(root)
            # fetch the id for the dataset in some way
            schema_path = root_path.joinpath("dataset.json")
            dataset_schema: types.DatasetSchema = dataset_schema_from_path(schema_path)
            schema_lookup[dataset_schema.id] = dataset_schema
    return schema_lookup


def profile_schema_from_file(filename: Union[Path, str]) -> Dict[str, types.ProfileSchema]:
    """Read a profile schema from a file on local drive."""
    with open(filename, "r") as file_handler:
        schema_info = json.load(file_handler)
        return {schema_info["name"]: types.ProfileSchema.from_dict(schema_info)}


@deprecated(
    version="1.0.4",
    reason="Does not work with datasets that have their tables split "
    "out into separate files. Use something like "
    "`schematools.cli._get_dataset_schema` instead.",
)
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


def get_rel_table_identifier(table_identifier: str, through_identifier: str) -> str:
    """Create identifier for related table (FK or M2M)."""
    return f"{table_identifier}_{through_identifier}"


def shorten_name(db_table_name: str, with_postfix: bool = False) -> str:
    """Shorten names to safe length for postgresql."""
    max_length = MAX_TABLE_NAME_LENGTH - int(with_postfix) * len(TMP_TABLE_POSTFIX)
    return db_table_name[:max_length]
