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
          AND column_name = 'id_big';
    """
    )
    record = results.fetchone()
    assert record.data_type == "bigint"
    assert record.numeric_precision == 64
