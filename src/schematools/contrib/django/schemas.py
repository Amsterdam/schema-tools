from __future__ import annotations

from typing import List

from more_ds.network.url import URL

from schematools import types
from schematools.utils import dataset_schema_from_url, dataset_schemas_from_url


def _setify(c):
    return set([] if c is None else c)


def get_schemas_for_url(
    schema_url: URL, limit_to: List[str] | None = None, skip: List[str] | None = None
) -> List[types.DatasetSchema]:
    """Gets all schemas from `schema_url`, or a subset, depending on `limit_to` and `skip`."""
    if not limit_to:
        schemas = [
            schema
            for schema in dataset_schemas_from_url(schema_url).values()
            if schema.id not in _setify(skip)
        ]
    else:
        limit_to_minus_skip = _setify(limit_to) - _setify(skip)
        schemas = [
            dataset_schema_from_url(schema_url, schema_id, prefetch_related=True)
            for schema_id in limit_to_minus_skip
        ]

    return schemas
