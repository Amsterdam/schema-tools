from datetime import date

from schematools.importer.base import BaseImporter
from schematools.importer.ndjson import NDJSONImporter


def test_camelcased_names_during_import(here, engine, bouwblokken_schema, dbsession):
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_bouwblokken ORDER BY id")]
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
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden-duplicate-id.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)


def test_skip_duplicate_keys_in_db_during_import_with_existing_value(
    here, engine, bouwblokken_schema, dbsession
):
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)


def test_skip_duplicate_keys_in_db_during_import_with_duplicate_in_next_batch(
    here, engine, bouwblokken_schema, dbsession
):
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(bouwblokken_schema, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.load_file(ndjson_path)


def test_numeric_datatype_scale(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that when multipleOf is used in schema,
    it's value is used to set the scale of the numeric datatype"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    record = [
        dict(r)
        for r in engine.execute(
            """
                                                SELECT data_type, numeric_scale
                                                FROM information_schema.columns
                                                WHERE 1=1
                                                AND table_schema = 'public'
                                                AND table_name = 'woningbouwplannen_woningbouwplan'
                                                AND column_name = 'avarage_sales_price';
                                                """
        )
    ]
    assert record[0]["data_type"] == "numeric"
    assert record[0]["numeric_scale"] == 4


def test_invalid_numeric_datatype_scale(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that when invaldi multipleOf i.e. 0.000 is used in schema,
    the datatype is in that case just plain numeric without scale"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    record = [
        dict(r)
        for r in engine.execute(
            """
                                                SELECT data_type, numeric_scale
                                                FROM information_schema.columns
                                                WHERE 1=1
                                                AND table_schema = 'public'
                                                AND table_name = 'woningbouwplannen_woningbouwplan'
                                                AND column_name = 'avarage_sales_price_incorrect'
                                                OR
                                                column_name = 'avarage_sales_price_incorrect_zero';
                                                """
        )
    ]
    assert record[0]["data_type"] == "numeric"
    assert not record[0]["numeric_scale"]
    assert record[1]["data_type"] == "numeric"
    assert not record[1]["numeric_scale"]


def test_biginteger_datatype(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that when biginter is used as datatype in schema, the datatype
    in the database is set to datatype int(8) instead of int(4)"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=True)
    results = engine.execute(
        """
        SELECT data_type, numeric_precision
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = 'woningbouwplannen_woningbouwplan'
        AND column_name = 'id';
    """
    )
    record = results.fetchone()
    assert record.data_type == "bigint"
    assert record.numeric_precision == 64


def test_add_column_comment(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a column comment is added as defined in the schema as field description"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=False)
    results = engine.execute(
        """
        SELECT pgd.description
        FROM pg_catalog.pg_statio_all_tables as st
        INNER JOIN pg_catalog.pg_description pgd on (pgd.objoid=st.relid)
        INNER JOIN information_schema.columns c on (pgd.objsubid=c.ordinal_position
        AND  c.table_schema=st.schemaname and c.table_name=st.relname)
        WHERE table_name  = 'woningbouwplannen_woningbouwplan'
        AND column_name = 'projectnaam';
    """
    )
    record = results.fetchone()
    assert record.description == "Naam van het project"


def test_add_table_comment(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a table comment is added as defined in the schema as table description"""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", ind_tables=True, ind_extra_index=False)
    results = engine.execute(
        """
        SELECT obj_description('public.woningbouwplannen_woningbouwplan'::regclass) as description;
    """
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
    engine.execute("CREATE SCHEMA IF NOT EXISTS schema_foo_bar;")
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects(
        "woningbouwplan", "schema_foo_bar", ind_tables=True, ind_extra_index=False
    )
    results = engine.execute(
        """
        SELECT schemaname FROM pg_tables WHERE tablename = 'woningbouwplannen_woningbouwplan'
    """
    )
    record = results.fetchone()
    assert record.schemaname == "schema_foo_bar"


def test_create_table_no_db_schema(here, engine, woningbouwplannen_schema, dbsession):
    """Prove that a table is created in DB schema public if no DB schema is given."""
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects("woningbouwplan", None, ind_tables=True, ind_extra_index=False)
    results = engine.execute(
        """
        SELECT schemaname FROM pg_tables WHERE tablename = 'woningbouwplannen_woningbouwplan'
    """
    )
    record = results.fetchone()
    assert record.schemaname == "public"


def test_create_table_temp_name(engine, woningbouwplannen_schema):
    """Prove that a table is created in DB schema with temporary name
    as definied in a dictionary."""
    table_temp_name = {"woningbouwplannen_woningbouwplan": "foo_bar"}
    importer = BaseImporter(woningbouwplannen_schema, engine)
    importer.generate_db_objects(
        "woningbouwplan",
        db_table_temp_name=table_temp_name,
        ind_tables=True,
        ind_extra_index=False,
    )
    results = engine.execute(
        """
        SELECT tablename FROM pg_tables WHERE tablename = 'foo_bar'
    """
    )
    record = results.fetchone()
    assert record.tablename == "foo_bar"
