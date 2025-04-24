from __future__ import annotations

import os
from pathlib import Path

from psycopg2 import sql

from schematools.types import _PUBLIC_SCOPE, DatasetSchema, DatasetTableSchema


def export_geopackages(
    connection,
    dataset_schema: DatasetSchema,
    output: str,
    table_ids: list[str] | None = None,
    scopes: list[str] | None = None,
    size: int | None = None,
) -> None:
    """Export geopackages for all tables or an indicated subset in the dataset.

    Args:
        connection: SQLAlchemy connection object. Is needed for the `psycopg2`
        formatting.
        dataset_schema: Schema that needs export as geopackage.
        output: path on the filesystem where output should be stored.
        table_ids: optional parameter for a subset for the tables of the datasetself.
        scopes: Keycloak scopes that need to be taken into account.
            The geopackage will be produced contains information that is only
            accessible with these scopes.
        size: To produce a subset of the rows, mainly for testing.
    """

    base_dir = Path(output)

    # Handle both SQLAlchemy Engine and Connection objects
    db_url = connection.engine.url if hasattr(connection, "engine") else connection.url

    tables = (
        dataset_schema.tables
        if not table_ids
        else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
    )
    command = 'ogr2ogr -f "GPKG" {output_path} PG:"{db_url}" -sql "{sql}"'
    for table in tables:
        output_path = base_dir / f"{table.db_name}.gpkg"
        field_names = sql.SQL(",").join(
            sql.Identifier(field.db_name)
            for field in _get_fields(dataset_schema, table, scopes)
            if field.db_name != "schema"
        )
        if not field_names.seq:
            continue
        table_name = sql.Identifier(table.db_name)
        query = sql.SQL("SELECT {field_names} from {table_name}").format(
            field_names=field_names, table_name=table_name
        )
        if size is not None:
            query = f"{query} LIMIT {size}"
        sql_stmt = query.as_string(connection.connection.cursor())
        os.system(  # noqa: S605  # nosec: B605
            command.format(output_path=output_path, db_url=db_url, sql=sql_stmt)  # noqa: S605
        )


def _get_fields(dataset_schema: DatasetSchema, table: DatasetTableSchema, scopes: list[str]):
    parent_scopes = set(dataset_schema.auth | table.auth) - {_PUBLIC_SCOPE}
    for field in table.fields:
        if field.is_array:
            continue
        if field.is_internal:
            continue
        if parent_scopes | set(field.auth) - {_PUBLIC_SCOPE} <= set(scopes):
            yield field
