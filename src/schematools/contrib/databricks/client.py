from databricks.sdk import WorkspaceClient

from schematools.contrib.databricks.types import DatabricksInfo, Tags


def get_databricks_info(full_name: str) -> DatabricksInfo:
    """
    Get table information from Databricks using the WorkspaceClient.

    Args:
        full_name (str): The full name of the table in the format "catalog.schema.table_name".

    Returns:
        DatabricksInfo: An object containing Databricks information.
    """
    client = WorkspaceClient()
    catalog, schema, table_name = full_name.split(".")
    table_info = client.tables.get(full_name, include_browse=True)
    table_tags = Tags.from_tag_assignments(client.entity_tag_assignments.list("tables", full_name))
    column_tags = {
        col.name: Tags.from_tag_assignments(
            client.entity_tag_assignments.list("columns", f"{full_name}.{col.name}")
        )
        for col in (table_info.columns if table_info.columns is not None else [])
        if col.name is not None
    }
    return DatabricksInfo(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        table_info=table_info,
        table_tags=table_tags,
        column_tags=column_tags,
    )
