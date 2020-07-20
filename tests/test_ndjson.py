from schematools.importer.ndjson import NDJSONImporter


def test_ndjson_import_nm(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "metingen.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_tables("metingen", truncate=True)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_metingen")]
    assert len(records) == 4
    # A non-object relation, should just lead to _id field
    assert "hoortbijmeetbout_id" in records[0]
    # check value from the ndjson input, should be string according to the schema
    assert records[0]["hoortbijmeetbout_id"] == "13881032"
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT * from meetbouten_metingen_meetbouten_referentiepunten"
        )
    ]
    # Should have a field 'identificatie' in the n-m table
    assert "identificatie" in records[0]


def test_ndjson_import_nm_compound_keys(here, engine, ggwgebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "ggwgebieden.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_tables("ggwgebieden", truncate=True)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden")]
    assert len(records) == 1
    # An "id" should have been generated, concat of the compound key fields
    assert "id" in records[0]
    assert records[0]["id"] == "03630950000000.1"
    records = [
        dict(r)
        for r in engine.execute("SELECT * from gebieden_ggwgebieden_gebieden_buurten")
    ]
    assert len(records) == 3
    assert records[0].keys() == {
        "buurten_id",
        "ggwgebieden_id",
        "identificatie",
        "volgnummer",
    }


def test_ndjson_import_1n(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_tables("meetbouten", truncate=True)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    # The foreign key, needed by Django, should be there
    assert "ligtinbuurt_id" in records[0]
    # And should have the concatenated value
    assert records[0]["ligtinbuurt_id"] == "10180001.1"
    # Should have a dunder-field identificatie
    assert "buurten_identificatie" in records[0]


def test_inactive_relation_that_are_commented_out(
    here, engine, stadsdelen_schema, dbsession
):
    """ Prove that objects that are commented out are flattened to strings """
    ndjson_path = here / "files" / "data" / "stadsdelen.ndjson"
    importer = NDJSONImporter(stadsdelen_schema, engine)
    importer.generate_tables("stadsdelen", truncate=True)
    importer.load_file(ndjson_path)
