import os

from databricks.sdk import WorkspaceClient

from schematools.contrib.databricks.types import DatabricksInfo, Tags

DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")


def get_databricks_info(full_name: str) -> DatabricksInfo:
    """
    Get table information from Databricks using the WorkspaceClient.

    Args:
        full_name (str): The full name of the table in the format "catalog.schema.table_name".

    Returns:
        DatabricksInfo: An object containing Databricks information.
    """
    if DATABRICKS_WAREHOUSE_ID is None:
        raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable is not set.")

    client = WorkspaceClient()
    catalog, schema, table_name = full_name.split(".")
    table_description = client.statement_execution.execute_statement(
        statement=f"DESCRIBE TABLE {full_name}", warehouse_id=DATABRICKS_WAREHOUSE_ID
    ).result
    if table_description is None:
        raise RuntimeError(f"Failed to retrieve table information for {full_name}.")

    table_tags = Tags.from_tag_assignments(client.entity_tag_assignments.list("tables", full_name))
    column_tags = {
        name: Tags.from_tag_assignments(
            client.entity_tag_assignments.list("columns", f"{full_name}.{name}")
        )
        for name, _, _ in (table_description.data_array or [])
        if name is not None
    }
    return DatabricksInfo(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        table_description=table_description,
        table_tags=table_tags,
        column_tags=column_tags,
    )
