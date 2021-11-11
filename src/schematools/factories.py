"""Module to hold factories."""
from collections import defaultdict
from typing import Dict, Optional, Set

from sqlalchemy import Column, MetaData, Table

from schematools import TMP_TABLE_POSTFIX, DATABASE_SCHEMA_NAME_DEFAULT
from schematools.importer import fetch_col_type
from schematools.types import DatasetSchema
from schematools.utils import to_snake_case


def tables_factory(
    dataset: DatasetSchema,
    metadata: Optional[MetaData] = None,
    db_table_names: Optional[Dict[str, Optional[str]]] = None,
    db_schema_names: Optional[Dict[str, Optional[str]]] = None,
    limit_tables_to: Optional[Set] = None,
) -> Dict[str, Table]:
    """Generate the SQLAlchemy Table objects base on a `DatasetSchema` definition.

    Args:
        dataset: The Amsterdam Schema definition of the dataset
        metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
        db_table_names: Optional sql table names, keyed on dataset_table_id.
            If not give, db_table_names are inferred from the schema name.
        db_schema_names: Optional database schema names, keyed on dataset_table_id.
            If not given, schema names default to `public`.
        limit_tables_to: Only process the indicated tables (based on table.id).

    The returned tables are keyed on the name of the dataset and table.
    SA Table objects are also created for the junction tables that are needed for relations.

    The nested and through tables that are generated on-the-fly are also taken into account.
    Special care is needed to add postfixes to tables names if the parent table of these
    tables has a non-default name. In that case, these on-the-fly tables also need to
    get an extra postfix in their db name.

    One caveat: The assumption now is that special "overridden" names for tables
    only are used because postfixes are added, and for no other reasons.
    """
    tables = defaultdict(dict)
    metadata = metadata or MetaData()

    for dataset_table in dataset.get_tables(include_nested=True, include_through=True):
        if limit_tables_to is not None and dataset_table.id not in limit_tables_to:
            continue
        db_table_description = dataset_table.description
        if (db_table_name := (db_table_names or {}).get(dataset_table.id)) is None:
            has_postfix = (
                db_schema_names is not None
                and (dataset_table.is_nested_table or dataset_table.is_through_table)
                and db_table_names.get(dataset_table.parent_table.id) is not None
            )
            postfix = TMP_TABLE_POSTFIX if has_postfix else None
            db_table_name = dataset_table.db_name(postfix=postfix)

        # If schema is None, default to Public. Leave it to None will have a risk that
        # the DB schema that is currently in use will be used to create the table in
        # leading to unwanted/unexepected results
        if (db_schema_name := (db_schema_names or {}).get(dataset_table.id)) is None:
            db_schema_name = DATABASE_SCHEMA_NAME_DEFAULT
        columns = []
        for field in dataset_table.fields:

            # Exclude nested and nm_relation fields (is_array check)
            # and fields that are added only for temporality
            if field.type.endswith("#/definitions/schema") or field.is_array or field.is_temporal:
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
            columns.append(
                Column(
                    f"{field_name}{id_postfix}", col_type, comment=field.description, **col_kwargs
                )
            )

        # The table_name has to be snakecased here, because
        # it is used to lookup an SA Table and it needs to match
        # with the records that are coming back from the
        # BaseImporter.
        tables[to_snake_case(dataset_table.name)] = Table(
            db_table_name,
            metadata,
            comment=db_table_description,
            schema=db_schema_name,
            *columns,
            extend_existing=True,
        )

    return tables
