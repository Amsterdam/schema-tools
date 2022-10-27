"""Module for SQLAlchemy-based database table creation."""
from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import DefaultDict, cast

from psycopg2 import sql
from sqlalchemy.sql.schema import Column, Index, MetaData, Table

from schematools import (
    DATABASE_SCHEMA_NAME_DEFAULT,
    MAX_TABLE_NAME_LENGTH,
    TABLE_INDEX_POSTFIX,
    TMP_TABLE_POSTFIX,
)
from schematools.importer import fetch_col_type
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema, DatasetTableSchema


def tables_factory(
    dataset: DatasetSchema,
    metadata: MetaData | None = None,
    db_table_names: dict[str, str | None] | None = None,
    db_schema_names: dict[str, str | None] | None = None,
    limit_tables_to: set | None = None,
    is_versioned_dataset: bool = False,
) -> dict[str, Table]:
    """Generate the SQLAlchemy Table objects base on a `DatasetSchema` definition.

    Args:
        dataset: The Amsterdam Schema definition of the dataset
        metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
        db_table_names: Optional sql table names, keyed on dataset_table_id.
            If not give, db_table_names are inferred from the schema name.
        db_schema_names: Optional database schema names, keyed on dataset_table_id.
            If not given, schema names default to `public`.
        limit_tables_to: Only process the indicated tables (based on table.id).
        is_versioned_dataset: Indicate whether the tables should be created in a private DB
            schema with a version in their name. See also:
            :attr:`.BaseImporter.is_versioned_dataset`. The private
            schema name will be derived from the dataset ID, unless overridden by the
            ``db_schema_name`` parameter.

    The returned tables are keyed on the name of the dataset and table.
    SA Table objects are also created for the junction tables that are needed for relations.

    The nested and through tables that are generated on-the-fly are also taken into account.
    Special care is needed to add postfixes to tables names if the parent table of these
    tables has a non-default name. In that case, these on-the-fly tables also need to
    get an extra postfix in their db name.

    One caveat: The assumption now is that special "overridden" names for tables
    only are used because postfixes are added, and for no other reasons.
    """
    tables = {}
    metadata = metadata or MetaData()

    for dataset_table in dataset.get_tables(include_nested=True, include_through=True):
        # The junction table for relations are imported separately nowadays.
        # However, nested tables are implemented using a sub-table
        # during import of the main table.
        # So, those nested tables need an SA table object to be able
        # to create the table where data has to be imported into.
        table_object_needed = (
            limit_tables_to is None
            or dataset_table.id in limit_tables_to
            or dataset_table.is_nested_table
            and dataset_table.parent_table.id in limit_tables_to  # type: ignore
        )

        if not table_object_needed:
            continue

        db_table_description = dataset_table.description
        if (db_table_name := (db_table_names or {}).get(dataset_table.id)) is None:
            has_postfix = (
                db_schema_names is not None
                and (dataset_table.is_nested_table or dataset_table.is_through_table)
                and (parent_table_name := db_table_names.get(dataset_table.parent_table.id))
                is not None
                and parent_table_name.endswith(TMP_TABLE_POSTFIX)
            )
            postfix = TMP_TABLE_POSTFIX if has_postfix else ""
            if is_versioned_dataset:
                db_table_name = dataset_table.db_name_variant(
                    with_dataset_prefix=False, with_version=True, postfix=postfix
                )
            else:
                db_table_name = dataset_table.db_name_variant(postfix=postfix)

        # If schema is None, default to Public. Leave it to None will have a risk that
        # the DB schema that is currently in use will be used to create the table in
        # leading to unwanted/unexepected results
        if (db_schema_name := (db_schema_names or {}).get(dataset_table.id)) is None:
            if is_versioned_dataset:
                db_schema_name = to_snake_case(dataset.id)
            else:
                db_schema_name = DATABASE_SCHEMA_NAME_DEFAULT
        columns = []
        for field in dataset_table.get_fields(include_subfields=True):

            # Exclude nested and nm_relation fields (is_array check)
            # and fields that are added only for temporality
            if (
                field.type.endswith("#/definitions/schema")
                or field.is_array
                or field.is_temporal_range
            ):
                continue
            try:
                col_type = fetch_col_type(field)
            except KeyError:
                raise NotImplementedError(
                    f'Import failed at "{field.id}": {dict(field)!r}\n'
                    f"Field type '{field.type}' is not implemented."
                ) from None
            col_kwargs = {"nullable": not field.required}
            if field.is_primary:
                col_kwargs["primary_key"] = True
                col_kwargs["nullable"] = False
                col_kwargs["autoincrement"] = field.type.endswith("autoincrement")
            columns.append(
                Column(field.db_name, col_type, comment=field.description, **col_kwargs)
            )

        alchemy_table = Table(
            db_table_name,
            metadata,
            *columns,
            comment=db_table_description,
            schema=db_schema_name,
            extend_existing=True,
        )
        alchemy_table.dataset_table = dataset_table
        tables[dataset_table.id] = alchemy_table

    return tables


def views_factory(dataset: DatasetSchema, tables: dict[str, Table]) -> dict[str, sql.SQL]:
    """Create VIEW statements.

    The views these statements define are there to provide the illusion that the tables they
    wrap are:

    - not versioned
    - do live in the "public" schema

    This illusion is needed for backwards compatability reasons.

    Args:
        dataset: The dataset currently being processed
        tables: The tables as generated by :func:`tables_factory`

    Returns: A dict mapping table names to VIEW statements.
    """
    dataset_tables: dict[str, DatasetTableSchema] = {
        dst.id: dst for dst in dataset.get_tables(include_nested=True, include_through=True)
    }
    if set(dataset_tables) != set(tables):
        raise ValueError(
            f"mismatch: dataset_tables has {set(dataset_tables)}, tables has {set(tables)}"
        )

    CREATE_VIEW = sql.SQL(
        """
        CREATE OR REPLACE VIEW "public".{view_name} AS
        SELECT *
          FROM {src_schema}.{src_table};
        """
    )
    views: dict[str, sql.SQL] = {
        tn: CREATE_VIEW.format(
            view_name=sql.Identifier(dataset_tables[tn].db_name),
            src_schema=sql.Identifier(tables[tn].schema),
            src_table=sql.Identifier(tables[tn].name),
        )
        for tn in dataset_tables
    }
    return views


def index_factory(
    dataset_table: DatasetTableSchema,
    metadata: MetaData | None = None,
    db_table_name: str | None = None,
    db_schema_name: str | None = None,
    is_versioned_dataset: bool = False,
) -> dict[str, list[Index]]:
    """Generates one or more SQLAlchemy Index objects to work with the JSON Schema.

    Args:
        dataset_table: The Amsterdam Schema definition of the table
        metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
        db_table_name: Optional table name, which is otherwise inferred from the schema name.
        db_schema_name: Optional database schema name, which is otherwise None and
            defaults to "public"
        is_versioned_dataset: Indicate whether the indices should be created in a private DB
            schema with a version in their name. See also:
            :attr:`.BaseImporter.is_versioned_dataset`. The private
            schema name will be derived from the dataset ID, unless overridden by the
            ``db_schema_name`` parameter.

    The returned Index objects are grouped by table names.
    """
    indexes: DefaultDict[str, list[Index]] = defaultdict(list)
    _metadata = cast(MetaData, metadata or MetaData())

    if is_versioned_dataset:
        if db_schema_name is None:
            # private DB schema instead of `public`
            db_schema_name = dataset_table.dataset.id
        if db_table_name is None:
            db_table_name = dataset_table.db_name_variant(
                # No dataset prefix as the tables will be created in their own
                # private schema.
                with_dataset_prefix=False,
                with_version=True,
            )
    else:
        if db_schema_name is None:
            db_schema_name = DATABASE_SCHEMA_NAME_DEFAULT
        if db_table_name is None:
            db_table_name = dataset_table.db_name

    table_name = f"{db_schema_name}.{db_table_name}"
    table: Table = _metadata.tables[table_name]

    indexes[db_table_name] = [
        _build_identifier_index(table, dataset_table, db_table_name),
        *_build_fk_indexes(table, dataset_table, db_table_name),
    ]

    through_indexes = _build_m2m_indexes(
        metadata, dataset_table, is_versioned_dataset, db_schema_name
    )
    for table_db_name, through_indexes in through_indexes.items():
        indexes[table_db_name].extend(through_indexes)

    return dict(indexes)


def _build_identifier_index(
    table_object: Table, dataset_table: DatasetTableSchema, db_table_name: str
) -> Index:
    """Creates index based on the 'identifier' specification in the Amsterdam schema."""
    identifier_columns: list[Column] = [
        table_object.c[identifier_field.db_name]
        for identifier_field in dataset_table.identifier_fields
    ]

    index_name = _format_index_name(f"{db_table_name}_identifier{TABLE_INDEX_POSTFIX}")
    return Index(index_name, *identifier_columns)


def _build_fk_indexes(
    table_object: Table, dataset_table: DatasetTableSchema, db_table_name: str
) -> list[Index]:
    """Creates an index on Foreign Keys."""
    return [
        Index(
            _format_index_name(f"{db_table_name}_{field.db_name}{TABLE_INDEX_POSTFIX}"),
            table_object.c[field.db_name],
        )
        for field in dataset_table.get_fields(include_subfields=True)
        if field.relation
    ]


def _build_m2m_indexes(
    metadata: MetaData,
    dataset_table: DatasetTableSchema,
    is_versioned_dataset: bool,
    db_schema_name: str,
) -> dict[str, list[Index]]:
    """Creates index(es) on the many-to-many tables.

    Those are based on 'relation' specification in the Amsterdam schema.
    """
    indexes: dict[str, list[Index]] = {}
    for field in dataset_table.fields:
        if not field.is_through_table:
            continue

        table = field.through_table

        # create the Index objects
        if is_versioned_dataset:
            table_db_name = table.db_name_variant(with_dataset_prefix=False, with_version=True)
        else:
            table_db_name = table.db_name

        table_id = f"{db_schema_name}.{table_db_name}"
        table_object = metadata.tables[table_id]
        indexes[table_db_name] = [
            Index(
                _format_index_name(f"{table_id}_{through_field.db_name}{TABLE_INDEX_POSTFIX}"),
                table_object.c[through_field.db_name],
            )
            for through_field in table.through_fields
        ]
    return indexes


def _format_index_name(index_name: str) -> str:
    """Create a hash value for index_name.

    Postgres DB holds currently 63 max characters for object names.
    To prevent exceeds and collisions, the index names are shortened
    based upon a hash.
    With the blake2s algorithm a digest size is set to 20 bytes,
    which produces a 40 character long hexadecimal string plus
    the additional 4 character postfix of '_idx' (TABLE_INDEX_POSTFIX).
    """
    if len(index_name) <= MAX_TABLE_NAME_LENGTH:
        return index_name
    else:
        return (
            hashlib.blake2s(index_name.encode(), digest_size=20).hexdigest() + TABLE_INDEX_POSTFIX
        )
