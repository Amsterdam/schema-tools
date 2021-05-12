"""Module to hold factories."""
from collections import defaultdict
from typing import Dict, Optional

from sqlalchemy import Column, MetaData, Table

from schematools.importer import fetch_col_type
from schematools.types import DatasetSchema
from schematools.utils import to_snake_case


def tables_factory(
    dataset: DatasetSchema,
    metadata: Optional[MetaData] = None,
) -> Dict[str, Table]:
    """Generate the SQLAlchemy Table objects base on a `DatasetSchema` definition.

    Params:
        dataset: The Amsterdam Schema definition of the dataset
        metadata: SQLAlchemy schema metadata that groups all tables to a single connection.

    The returned tables are keyed on the name of the dataset and table.
    SA Table objects are also created for the junction tables that are needed for relations.
    """
    tables = defaultdict(dict)
    metadata = metadata or MetaData()

    for dataset_table in dataset.get_tables(include_nested=True, include_through=True):
        db_table_name: str = dataset_table.db_name()
        table_id: str = dataset_table.id
        columns = []
        for field in dataset_table.fields:
            # Exclude nested and nm_relation fields (is_array check)
            if field.type.endswith("#/definitions/schema") or field.is_array:
                continue
            field_name = to_snake_case(field.name)

            try:
                col_type = fetch_col_type(field)
            except KeyError:
                raise NotImplementedError(
                    f'Import failed at "{field.name}": {dict(field)!r}\n'
                    f"Field type '{field.type}' is not implemented."
                ) from None

            col_kwargs = {"nullable": not field.required}
            if field.is_primary:
                col_kwargs["primary_key"] = True
                col_kwargs["nullable"] = False
                col_kwargs["autoincrement"] = field.type.endswith("autoincrement")

            id_postfix = "_id" if field.relation else ""

            columns.append(Column(f"{field_name}{id_postfix}", col_type, **col_kwargs))

        tables[table_id] = Table(db_table_name, metadata, *columns, extend_existing=True)

    return tables
