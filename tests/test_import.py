from __future__ import annotations

from datetime import date
from typing import Final

from sqlalchemy import Boolean, text
from sqlalchemy.sql.ddl import CreateSchema
from sqlalchemy.sql.elements import TextClause

from schematools.importer.base import BaseImporter
from schematools.importer.ndjson import NDJSONImporter

SCHEMA_EXISTS: Final[TextClause] = text(
    """
    SELECT EXISTS(SELECT schema_name
              FROM information_schema.schemata
              WHERE schema_name = :schema_name) AS exists
    """
).columns(exists=Boolean)
TABLE_EXISTS: Final[TextClause] = text(
    """
    SELECT EXISTS(SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = :schema_name
                AND table_name = :table_name
                AND table_type = 'BASE TABLE') AS exists;
    """
).columns(exists=Boolean)
VIEW_EXISTS: Final[TextClause] = text(
    """
    SELECT EXISTS(SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = :schema_name
                AND table_name = :view_name
                AND table_type = 'VIEW') AS exists;
    """
).columns(exists=Boolean)


def test_camelcased_names_during_import(here, engine, bouwblokken_schema, dbsession):
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    last_record = importer.load_file(ndjson_path)
    assert dict(last_record) == {
        "begin_geldigheid": "2008-03-12",
        "eind_geldigheid": "2010-10-19",
        "id": 2,
        "ligt_in_buurt_id": 34,
    }
    assert last_record.source == {
        "beginGeldigheid": "2008-03-12",
        "eindgeldigheid": "2010-10-19",
        "id": 2,
        "ligtinbuurt": 34,
        "schema": "irrelevant",
    }
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM bouwblokken_bouwblokken_v1 ORDER BY id"))
            .mappings()
            .all()
        )
    assert len(records) == 2
    assert set(records[0].keys()) == {
        "id",
        "begin_geldigheid",
        "eind_geldigheid",
        "ligt_in_buurt_id",
    }
    assert records[0]["begin_geldigheid"] == date(2006, 1, 12)
    assert records[0]["eind_geldigheid"] == date(2008, 11, 14)


def test_skip_duplicate_keys_in_batch_during_import(here, engine, bouwblokken_schema, dbsession):
    """Prove that the ndjson, which has a duplicate record, does not lead to an exception"""
    ndjson_path = here / "files" / "data" / "gebieden-duplicate-id.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    last_record = importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM bouwblokken_bouwblokken_v1 ORDER BY id"))
            .mappings()
            .all()
        )
    assert records == [
        # Only one inserted, and no crash happened.
        {
            "begin_geldigheid": date(2006, 1, 12),
            "eind_geldigheid": date(2008, 11, 14),
            "id": "1",
            "ligt_in_buurt_id": "12",
        }
    ]

    # Despite not being inserted, the record should still be returned as last one,
    # because this the last one processed (useful for cursor tracking).
    assert last_record == {
        "begin_geldigheid": "2008-03-12",
        "eind_geldigheid": "2010-10-19",
        "id": 1,
        "ligt_in_buurt_id": 34,
    }


def test_skip_duplicate_keys_in_db_during_import_with_existing_value(
    here, engine, bouwblokken_schema, dbsession
):
    """Prove that the ndjson, which has a duplicate record, does not lead to an exception"""
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)


def test_skip_duplicate_keys_in_db_during_import_with_duplicate_in_next_batch(
    here, engine, bouwblokken_schema, dbsession
):
    """Prove that the ndjson, which has a duplicate record, does not lead to an exception"""
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.load_file(ndjson_path)


def test_numeric_datatype_scale(
    here, engine, woningbouwplannen_schema, gebieden_schema, dbsession
):
    """Prove that when multipleOf is used in schema,
    it's value is used to set the scale of the numeric datatype"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    with engine.begin() as conn:
        records = (
            conn.execute(
                text(
                    """
                SELECT data_type, numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'woningbouwplannen_woningbouwplan_v1'
                      AND column_name = 'avarage_sales_price';
                """
                )
            )
            .mappings()
            .all()
        )
    assert records[0]["data_type"] == "numeric"
    assert records[0]["numeric_scale"] == 4


def test_invalid_numeric_datatype_scale(
    here, engine, woningbouwplannen_schema, gebieden_schema, dbsession
):
    """Prove that when invaldi multipleOf i.e. 0.000 is used in schema,
    the datatype is in that case just plain numeric without scale"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    with engine.begin() as conn:
        record = (
            conn.execute(
                text(
                    """
                SELECT data_type, numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                        AND table_name = 'woningbouwplannen_woningbouwplan_v1'
                        AND column_name = 'avarage_sales_price_incorrect'
                       OR column_name = 'avarage_sales_price_incorrect_zero'
                """
                )
            )
            .mappings()
            .all()
        )
    assert record[0]["data_type"] == "numeric"
    assert not record[0]["numeric_scale"]
    assert record[1]["data_type"] == "numeric"
    assert not record[1]["numeric_scale"]


def test_biginteger_datatype(here, engine, woningbouwplannen_schema, gebieden_schema, dbsession):
    """Prove that when biginter is used as datatype in schema, the datatype
    in the database is set to datatype int(8) instead of int(4)"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    with engine.begin() as conn:
        results = conn.execute(
            text(
                """
            SELECT data_type, numeric_precision
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'woningbouwplannen_woningbouwplan_v1'
                  AND column_name = 'id';
            """
            )
        )
        record = results.fetchone()
    assert record.data_type == "bigint"
    assert record.numeric_precision == 64


def test_add_column_comment(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a column comment is added as defined in the schema as field description"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=False)
    with engine.begin() as conn:
        results = conn.execute(
            text(
                """
            SELECT pgd.description
                FROM pg_catalog.pg_statio_all_tables AS st
                         INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid = st.relid)
                         INNER JOIN information_schema.columns c ON (pgd.objsubid = c.ordinal_position
                    AND c.table_schema = st.schemaname AND c.table_name = st.relname)
                WHERE table_name = 'woningbouwplannen_woningbouwplan_v1'
                  AND column_name = 'projectnaam';
            """
            )
        )
        record = results.fetchone()
    assert record.description == "Naam van het project"


def test_add_table_comment(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a table comment is added as defined in the schema as table description"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=False)
    with engine.begin() as conn:
        results = conn.execute(
            text(
                """
            SELECT OBJ_DESCRIPTION('public.woningbouwplannen_woningbouwplan_v1'::REGCLASS) AS description;
            """
            )
        )
        record = results.fetchone()
    assert (
        record.description == "De aantallen vormen de planvoorraad. "
        "Dit zijn niet de aantallen die definitief worden gerealiseerd. "
        "Ervaring leert dat een deel van de planvoorraad wordt opgeschoven. "
        "Niet alle woningbouw initiatieven doorlopen de verschillende plaberumfasen. "
        "Met name kleinere particuliere projecten worden in de regel pas toegevoegd aan "
        "de monitor zodra er een intentieovereenkomst of afsprakenbrief is getekend."
    )


def test_create_table_db_schema(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a table is created in given DB schema."""
    with engine.begin() as conn:
        conn.execute(CreateSchema("schema_foo_bar", if_not_exists=True))

    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects(
        "woningbouwplan", "schema_foo_bar", ind_tables=True, ind_extra_index=False
    )
    with engine.begin() as conn:
        results = conn.execute(
            text(
                """
            SELECT schemaname FROM pg_tables WHERE tablename = 'woningbouwplannen_woningbouwplan_v1'
            """
            )
        )
        record = results.fetchone()
    assert record.schemaname == "schema_foo_bar"


def test_create_table_no_db_schema(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a table is created in DB schema public if no DB schema is given."""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", None, ind_tables=True, ind_extra_index=False)

    with engine.begin() as conn:
        results = conn.execute(
            text(
                """
            SELECT schemaname FROM pg_tables WHERE tablename = 'woningbouwplannen_woningbouwplan_v1'
            """
            )
        )
        record = results.fetchone()
    assert record.schemaname == "public"


def test_generate_db_objects_is_versioned_dataset(
    here, engine, woningbouwplannen_schema, dbsession
):
    """Prove that dataset is created in private DB schema with versioned tables."""
    with engine.connect() as conn:
        assert not conn.scalar(SCHEMA_EXISTS, {"schema_name": "woningbouwplannen"})

        importer = BaseImporter(woningbouwplannen_schema, engine)
        importer.generate_db_objects(
            "woningbouwplan", ind_tables=True, ind_extra_index=False, is_versioned_dataset=True
        )
        assert conn.scalar(SCHEMA_EXISTS, {"schema_name": "woningbouwplannen"})
        for table_name in (
            "woningbouwplan_v1",
            "woningbouwplan_buurten_v1",
            "woningbouwplan_buurten_as_scalar_v1",
        ):
            assert conn.scalar(
                TABLE_EXISTS, {"schema_name": "woningbouwplannen", "table_name": table_name}
            )
        for view_name in (
            "woningbouwplannen_woningbouwplan_v1",
            "woningbouwplannen_woningbouwplan_buurten_v1",
            "woningbouwplannen_woningbouwplan_buurten_as_scalar_v1",
        ):
            assert conn.scalar(VIEW_EXISTS, {"schema_name": "public", "view_name": view_name})


def test_create_table_temp_name(engine, db_schema, woningbouwplannen_schema, gebieden_schema):
    """Prove that a table is created in DB schema with the temporary name
    as definied in a dictionary."""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects(
        "woningbouwplan",
        db_table_name="foo_bar",
        ind_tables=True,
        ind_extra_index=False,
    )

    with engine.begin() as conn:
        results = conn.execute(
            text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'foo_%%'")
        )
        names = {r[0] for r in results.fetchall()}
    assert names == {"foo_bar"}
