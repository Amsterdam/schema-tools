import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from schematools.contrib.databricks.types import DatabricksInfo, Tags

DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")


TABLE_DATA_SQL = """
    SELECT
    t.comment,
    collect_list(
    named_struct(
        'name', tt.tag_name,
        'value', tt.tag_value
    )
    ) FILTER (WHERE tt.tag_name IS NOT NULL) AS tags
    FROM system.information_schema.tables AS t
    LEFT JOIN system.information_schema.table_tags AS tt
    ON tt.catalog_name = t.table_catalog
    AND tt.schema_name = t.table_schema
    AND tt.table_name = t.table_name
    WHERE t.table_catalog = :catalog
    AND t.table_schema = :schema
    AND t.table_name = :table_name
    GROUP BY t.comment;
"""

COLUMN_DATA_SQL = """
    SELECT
    c.column_name,
    c.full_data_type,
    c.is_nullable,
    c.column_default,
    c.comment,
    collect_list(
        named_struct(
        'name', ct.tag_name,
        'value', ct.tag_value
        )
    ) FILTER (WHERE ct.tag_name IS NOT NULL) AS tags
    FROM system.information_schema.columns AS c
    LEFT JOIN system.information_schema.column_tags AS ct
    ON ct.catalog_name = c.table_catalog
    AND ct.schema_name = c.table_schema
    AND ct.table_name = c.table_name
    AND ct.column_name = c.column_name
    WHERE c.table_catalog = :catalog
    AND c.table_schema = :schema
    AND c.table_name = :table_name
    GROUP BY
        c.column_name,
        c.ordinal_position,
        c.full_data_type,
        c.is_nullable,
        c.column_default,
        c.comment
    ORDER BY c.ordinal_position;
"""


def _execute_sql(
    client,
    sql_statement: str,
    parameters: list[StatementParameterListItem] | None = None,
):
    result = client.statement_execution.execute_statement(
        statement=sql_statement,
        parameters=parameters,
        warehouse_id=DATABRICKS_WAREHOUSE_ID,
        wait_timeout="50s",
    ).result
    if result is None:
        raise RuntimeError("Failed to retrieve information from databricks.")
    return result.data_array


def get_databricks_info(catalog: str, schema: str, table_name: str) -> DatabricksInfo:
    """
    Get table information from Databricks using the WorkspaceClient.

    Args:
        catalog (str): The catalog of the table.
        schema (str): The schema of the table.
        table_name (str): The name of the table.

    Returns:
        DatabricksInfo: An object containing Databricks information.
    """
    if DATABRICKS_WAREHOUSE_ID is None:
        raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable is not set.")
    parameters = [
        StatementParameterListItem(name="catalog", value=catalog),
        StatementParameterListItem(name="schema", value=schema),
        StatementParameterListItem(name="table_name", value=table_name),
    ]
    client = WorkspaceClient()
    table_data = _execute_sql(client, TABLE_DATA_SQL, parameters=parameters)
    column_data = _execute_sql(client, COLUMN_DATA_SQL, parameters=parameters)
    table_tags = Tags.from_tag_list(
        json.loads(table_data[0][1]) if table_data and table_data[0][1] is not None else [],
        "tables",
    )
    column_tags = {
        col[0]: Tags.from_tag_list(
            json.loads(col[-1]) if column_data and col[-1] is not None else [],
            "columns",
        )
        for col in (column_data or [])
        if col[0] is not None
    }
    return DatabricksInfo(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        table_data=table_data[0] if table_data else None,
        column_data=column_data if column_data else [],
        table_tags=table_tags,
        column_tags=column_tags,
    )
