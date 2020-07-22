from schematools.importer.ndjson import NDJSONImporter


def test_ndjson_import_nm(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "metingen.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.load_file(ndjson_path, "metingen", truncate=True)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_metingen")]
    assert len(records) == 4
    # A non-object relation, should just lead to _id field
    assert "hoortbijmeetbout_id" in records[0]
    # check value from the ndjson input, should be string according to the schema
    assert records[0]["hoortbijmeetbout_id"] == "13881032"
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT * from meetbouten_metingen_refereertaanreferentiepunten"
        )
    ]
    # Should have a field 'identificatie' in the n-m table
    assert "identificatie" in records[0]


def test_ndjson_import_1n(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.load_file(ndjson_path, "meetbouten", truncate=True)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    # The foreign key, needed by Django, should be there
    assert "ligtinbuurt_id" in records[0]
    # And should have the concatenated value
    assert records[0]["ligtinbuurt_id"] == "10180001.1"
    # Should have a dunder-field identificatie
    assert "buurten__identificatie" in records[0]
