"""Module for SQLAlchemy-based database table creation."""

from __future__ import annotations

import hashlib
import logging
import numbers
from collections import defaultdict
from decimal import Decimal
from typing import cast

from geoalchemy2.types import Geometry
from psycopg import sql
from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, Float, Numeric, String, Time
from sqlalchemy.sql.schema import Column, Index, MetaData, Table
from sqlalchemy.types import ARRAY

from schematools import (
    DATABASE_SCHEMA_NAME_DEFAULT,
    MAX_TABLE_NAME_LENGTH,
    SRID_3D,
    TABLE_INDEX_POSTFIX,
    TMP_TABLE_POSTFIX,
)
from schematools.naming import to_snake_case
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema

logger = logging.getLogger(__name__)

FORMAT_MODELS_LOOKUP = {
    "date": Date,
    "time": Time,
    "date-time": DateTime,
    "uri": String,
    "email": String,
    "json": JSON,
}

JSON_TYPE_TO_PG = {
    "string": String,
    "object": String,
    "boolean": Boolean,
    "integer": BigInteger,
    "integer/autoincrement": BigInteger,
    "number": Float,
    "array": ARRAY(String),
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema": String,
}

GEOJSON_TYPE_TO_WKT = {
    "https://geojson.org/schema/Geometry.json": "GEOMETRY",
    "https://geojson.org/schema/Point.json": "POINT",
    "https://geojson.org/schema/Polygon.json": "POLYGON",
    "https://geojson.org/schema/MultiPolygon.json": "MULTIPOLYGON",
    "https://geojson.org/schema/MultiPoint.json": "MULTIPOINT",
    "https://geojson.org/schema/LineString.json": "LINESTRING",
    "https://geojson.org/schema/MultiLineString.json": "MULTILINESTRING",
}


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
    dataset_tables = dataset.get_tables(include_nested=True, include_through=True)

    # For compatibility and consistency, allow snake-cased table ID's
    db_table_names = _snake_keys(db_table_names) if db_table_names else {}
    db_schema_names = _snake_keys(db_schema_names) if db_schema_names else {}

    # Validate 'limit_tables_to' to avoid unexpected KeyError for the insert_statements
    # later on during the import, as that is based on the tables being generated here.
    if limit_tables_to is not None:
        table_ids = {table.id for table in dataset_tables}
        invalid = set(limit_tables_to) - table_ids - {to_snake_case(id) for id in table_ids}
        if invalid:
            raise ValueError(
                f"limit_tables_to has invalid entries: {', '.join(sorted(invalid))}. "
                f"Available are: {', '.join(sorted(table_ids))}"
            )
        # For compatibility and consistency, the check works on snake-cased identifiers.
        # This is needed because get_table_by_id() also still accepts those values.
        # Otherwise, generate_db_objects(table_id=.., limit_tables_to=..) allows snake-cased
        # identifiers for the first parameter, but needs exact-cased ids for the last parameter.
        limit_tables_to = {to_snake_case(id) for id in limit_tables_to}

    for dataset_table in dataset_tables:
        snaked_table_id = to_snake_case(dataset_table.id)
        snaked_parent_table_id = (
            to_snake_case(dataset_table.parent_table.id)
            if dataset_table.is_nested_table or dataset_table.is_through_table
            else None
        )

        # The junction table for relations are imported separately nowadays.
        # However, nested tables are implemented using a sub-table
        # during import of the main table.
        # So, those nested tables need an SA table object to be able
        # to create the table where data has to be imported into.
        table_object_needed = (
            limit_tables_to is None
            or snaked_table_id in limit_tables_to
            or (dataset_table.is_nested_table and snaked_parent_table_id in limit_tables_to)
        )

        if not table_object_needed:
            logger.debug("tables_factory() - skipping table %s", dataset_table.qualified_id)
            continue

        db_table_description = dataset_table.description
        if (db_table_name := db_table_names.get(snaked_table_id)) is None:
            # No predefined table name, generate a custom one.
            # When the parent has a _new postfix, make sure nested tables also receive that.
            has_postfix = (
                db_schema_names
                and snaked_parent_table_id
                and (parent_table_name := db_table_names.get(snaked_parent_table_id)) is not None
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
        if (db_schema_name := db_schema_names.get(snaked_table_id)) is None:
            if is_versioned_dataset:
                db_schema_name = to_snake_case(dataset.id)
            else:
                db_schema_name = DATABASE_SCHEMA_NAME_DEFAULT

        columns = [_column_factory(field) for field in dataset_table.get_db_fields()]

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


def _column_factory(field: DatasetFieldSchema) -> Column:
    """Generate a SQLAlchemy column for a single field"""
    try:
        col_type = _get_col_type(field)
    except KeyError:
        raise NotImplementedError(
            f"Field '{field.qualified_id}' type '{field.type}' is not implemented."
        ) from None

    col_kwargs = {"nullable": not field.required}
    if field.is_primary:
        col_kwargs["primary_key"] = True
        col_kwargs["nullable"] = False
        col_kwargs["autoincrement"] = field.type.endswith("autoincrement")

    return Column(field.db_name, col_type, comment=field.description, **col_kwargs)


def _get_col_type(field: DatasetFieldSchema):
    if (field_format := field.format) is not None:
        return FORMAT_MODELS_LOOKUP[field_format]

    # TODO: format takes precedence over multipleof
    # if there is an use case that both can apply for a field definition
    # then logic must be changed
    field_multiple = field.multipleof
    if field_multiple is not None:
        return _numeric_datatype_scale(scale_=field_multiple)

    if field.is_geo:
        is_3d = field.srid in SRID_3D
        return Geometry(
            geometry_type=GEOJSON_TYPE_TO_WKT[field.type],
            srid=field.srid,
            dimension=3 if is_3d else 2,
            spatial_index=False,  # Done manually below for control over index naming
            # use_N_D_index=field.srid in SRID_3D,
        )

    return JSON_TYPE_TO_PG[field.type]


def _numeric_datatype_scale(scale_=None):
    """detect scale from decimal for database datatype scale definition"""
    if (
        isinstance(scale_, numbers.Number)
        and str(scale_).count("1") == 1
        and str(scale_).endswith("1")
    ):
        # TODO: make it possible to set percision too
        # now it defaults to max of 12
        get_scale = Decimal(str(scale_)).as_tuple().exponent
        if get_scale < 0:
            get_scale = get_scale * -1
        return Numeric(precision=12, scale=get_scale)
    else:
        return Numeric


def _snake_keys(value: dict[str, str]) -> dict[str, str]:
    return {to_snake_case(table_id): value for table_id, value in value.items()}


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
    indexes: defaultdict[str, list[Index]] = defaultdict(list)
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

    table_id = f"{db_schema_name}.{db_table_name}"
    try:
        table: Table = _metadata.tables[table_id]
    except KeyError:
        # When tables_factory() is called with limit_tables_to, it's possible that this
        # main table is not created, and only the M2m are created in a separate import call.
        logger.warning("Table '%s' not found...skipping index creation", table_id)
    else:
        indexes[db_table_name] = [
            _build_identifier_index(table, dataset_table, db_table_name),
            *_build_fk_indexes(table, dataset_table, db_table_name),
            *_build_geo_indexes(table, dataset_table, db_table_name),
            *build_temporal_indexes(table, dataset_table, db_table_name),
        ]

    m2m_indexes = _build_m2m_indexes(metadata, dataset_table, is_versioned_dataset, db_schema_name)
    for table_db_name, through_indexes in m2m_indexes.items():
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


def _build_geo_indexes(
    table_object: Table, dataset_table: DatasetTableSchema, db_table_name: str
) -> list[Index]:
    """Creates an index on geometry fields."""
    return [
        Index(
            _format_index_name(f"{db_table_name}_{field.db_name}{TABLE_INDEX_POSTFIX}"),
            table_object.c[field.db_name],
            # Taken from geoalchemy2:
            postgresql_using="gist",
            postgresql_ops=(
                {field.db_name: "gist_geometry_ops_nd"} if field.srid in SRID_3D else {}
            ),
        )
        for field in dataset_table.fields
        if field.is_geo
    ]


def build_temporal_indexes(
    table_object: Table, dataset_table: DatasetTableSchema, db_table_name: str
) -> list[Index]:
    """Creates an index on temporal fields."""
    if dataset_table.temporal:
        fields = set()
        for field in dataset_table._temporal_range_field_ids:
            try:
                fields.add(table_object.c[to_snake_case(field)])
            except KeyError:
                logger.warning("Field '%s' not found...skipping temporal index creation", field)
                return []

        combined_index = Index(
            _format_index_name(f"{db_table_name}_temporal{TABLE_INDEX_POSTFIX}"), *fields
        )

        fields_index = [
            Index(
                _format_index_name(f"{db_table_name}_{field.db_name}{TABLE_INDEX_POSTFIX}"),
                table_object.c[field.db_name],
            )
            for field in dataset_table.temporal.temporal_fields
        ]

        return [combined_index, *fields_index]
    else:
        return []


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
        try:
            table_object = metadata.tables[table_id]
        except KeyError:
            # When tables_factory() is called with limit_tables_to, it's possible that this M2M
            # table is not created yet, and will be created with a separate import call.
            logger.warning("Table '%s' not found...skipping M2M index creation", table_id)
            continue

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
