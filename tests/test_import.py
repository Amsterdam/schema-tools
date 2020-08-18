from datetime import date
from schematools.importer.ndjson import NDJSONImporter


def test_camelcased_names_during_import(here, engine, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.load_file(ndjson_path, "bouwblokken", truncate=True)
    records = [
        dict(r)
        for r in engine.execute("SELECT * from gebieden_bouwblokken ORDER BY id")
    ]
    assert len(records) == 2
    assert set(records[0].keys()) == {
        "id",
        "begin_geldigheid",
        "eind_geldigheid",
    }
    assert records[0]["begin_geldigheid"] == date(2006, 1, 12)
    assert records[0]["eind_geldigheid"] == date(2008, 11, 14)
