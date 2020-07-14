from schematools.types import DatasetTableSchema


def get_table_name(dataset_table: DatasetTableSchema) -> str:
    """Generate the database identifier for the table."""
    schema = dataset_table._parent_schema
    return f"{schema.id}_{dataset_table.id}".replace("-", "_")
