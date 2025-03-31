from __future__ import annotations

import os
from io import StringIO
from pathlib import Path

from psycopg import sql

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
        connection: SQLAlchemy connection object. Is needed for the SQL formatting.
        dataset_schema: Schema that needs export as geopackage.
        output: path on the filesystem where output should be stored.
        table_ids: optional parameter for a subset for the tables of the datasetself.
        scopes: Keycloak scopes that need to be taken into account.
            The geopackage will be produced contains information that is only
            accessible with these scopes.
        size: To produce a subset of the rows, mainly for testing.
    """

    base_dir = Path(output)

    pg_conn_str = (
        f"host={connection.engine.url.host} "
        f"port={connection.engine.url.port} "
        f"dbname={connection.engine.url.database} "
        f"user={connection.engine.url.username} "
        f"password={connection.engine.url.password}"
    )

    tables = (
        dataset_schema.tables
        if not table_ids
        else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
    )

    for table in tables:
        output_path = base_dir / f"{table.db_name.replace('_v1', '')}.gpkg"
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
            query = sql.SQL("{query} LIMIT {size}").format(query=query, size=sql.Literal(size))

        copy_sql = sql.SQL("COPY ({query}) TO STDOUT").format(query=query)

        with connection.connection.cursor() as cursor:
            cursor.copy_expert(copy_sql, StringIO())
            sql_stmt = query.as_string(cursor)

        os.system(  # noqa: S605  # nosec: B605
            f'ogr2ogr -f "GPKG" {output_path} PG:"{pg_conn_str}" -sql "{sql_stmt}"'
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
