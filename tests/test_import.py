from datetime import date
from schematools.importer.ndjson import NDJSONImporter


def test_camelcased_names_during_import(here, engine, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)
    records = [
        dict(r)
        for r in engine.execute("SELECT * from gebieden_bouwblokken ORDER BY id")
    ]
    assert len(records) == 2
    assert set(records[0].keys()) == {
        "id",
        "begin_geldigheid",
        "eind_geldigheid",
        "ligt_in_buurt_id",
    }
    assert records[0]["begin_geldigheid"] == date(2006, 1, 12)
    assert records[0]["eind_geldigheid"] == date(2008, 11, 14)


def test_skip_duplicate_keys_in_batch_during_import(
    here, engine, gebieden_schema, dbsession
):
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden-duplicate-id.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)


def test_skip_duplicate_keys_in_db_during_import_with_existing_value(
    here, engine, gebieden_schema, dbsession
):
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)


def test_skip_duplicate_keys_in_db_during_import_with_duplicate_in_next_batch(
    here, engine, gebieden_schema, dbsession
):
    """ Prove that the ndjson, which has a duplicate record, does not lead to an exception """
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)
    importer.load_file(ndjson_path)
