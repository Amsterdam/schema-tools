import datetime

from schematools.importer.ndjson import NDJSONImporter


def test_ndjson_import_nm(here, engine, meetbouten_schema, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "metingen.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_metingen")]
    assert len(records) == 4
    # A non-object relation, should just lead to _id field
    assert "hoortbijmeetbout_id" in records[0]
    # check value from the ndjson input, should be string according to the schema
    assert records[0]["hoortbijmeetbout_id"] == "13881032"
    records = [
        dict(r)
        for r in engine.execute("SELECT * from meetbouten_metingen_refereertaanreferentiepunten")
    ]
    # Should have a field 'id' in the n-m table
    # an extra _identificatie is not needed, this is not a composite key
    assert "refereertaanreferentiepunten_id" in records[0]


def test_ndjson_import_separate_relations_target_composite(
    here, engine, meetbouten_schema, gebieden_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "meetbouten_ligt_in_buurt.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten_ligt_in_buurt", truncate=True, ind_extra_index=False)

    importer.load_file(ndjson_path, is_through_table=True)
    records = [
        dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten_ligt_in_buurt")
    ]
    assert records == [
        {
            "id": 1,
            "meetbouten_id": "25281132",
            "ligt_in_buurt_identificatie": "03630023753951",
            "ligt_in_buurt_volgnummer": 1,
            "ligt_in_buurt_id": "03630023753951.1",
            "begin_geldigheid": datetime.date(2015, 1, 1),
            "eind_geldigheid": None,
        },
    ]


def test_ndjson_import_separate_relations_both_composite(
    here, engine, ggwgebieden_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "ggwgebieden_bestaatuitbuurten.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects(
        "ggwgebieden_bestaatuitbuurten", truncate=True, ind_extra_index=False
    )

    importer.load_file(ndjson_path, is_through_table=True)
    records = [
        dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden_bestaatuitbuurten")
    ]

    assert records == [
        {
            "id": 1,
            "ggwgebieden_identificatie": "012",
            "ggwgebieden_volgnummer": 1,
            "ggwgebieden_id": "012.1",
            "bestaatuitbuurten_identificatie": "045",
            "bestaatuitbuurten_volgnummer": 2,
            "bestaatuitbuurten_id": "045.2",
        },
    ]


def test_ndjson_import_no_embedded_relation_in_data(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten-no-embedded-rel-data.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    # The foreign key, needed by Django, should be there
    assert "ligt_in_buurt_id" in records[0]
    # And should be None
    assert records[0]["ligt_in_buurt_id"] is None
    # Should have a field identificatie
    assert "ligt_in_buurt_identificatie" in records[0]
    # That is also None
    assert records[0]["ligt_in_buurt_identificatie"] is None


def test_ndjson_import_no_embedded_nm_relation_in_data(
    here, engine, ggwgebieden_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "ggwgebieden-no-embedded-rel-data.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)

    importer.load_file(ndjson_path, is_through_table=True)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden")]
    assert len(records) == 1

    # Through table is available, but has no content
    # (because there is no data  for it in the ndjson).
    records = [
        dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden_bestaatuitbuurten")
    ]
    assert len(records) == 0


def test_ndjson_import_jsonpath_provenance(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    assert records[0]["merk_code"] == "12"
    assert records[0]["merk_omschrijving"] == "De meetbout"


def test_ndjson_import_nm_composite_keys(here, engine, ggwgebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "ggwgebieden.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden")]
    assert len(records) == 1
    # An "id" should have been generated, concat of the composite key fields
    assert "id" in records[0]
    assert records[0]["id"] == "03630950000000.1"
    records = [
        dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden_bestaatuitbuurten")
    ]
    assert len(records) == 3
    # Also the temporal fields are present in the database
    columns = {
        "id",
        "ggwgebieden_id",
        "bestaatuitbuurten_id",
        "ggwgebieden_volgnummer",
        "ggwgebieden_identificatie",
        "bestaatuitbuurten_identificatie",
        "bestaatuitbuurten_volgnummer",
    }

    assert records[0].keys() == columns


def test_ndjson_import_nm_composite_keys_with_geldigheid(here, engine, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "ggwgebieden-with-geldigheid.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden")]
    assert len(records) == 1
    # An "id" should have been generated, concat of the composite key fields
    assert "id" in records[0]
    assert records[0]["id"] == "03630950000000.1"

    records = [
        dict(r) for r in engine.execute("SELECT * from gebieden_ggwgebieden_bestaat_uit_buurten")
    ]
    assert len(records) == 3
    # Also the temporal fields are present in the database
    columns = {
        "id",
        "ggwgebieden_id",
        "bestaat_uit_buurten_id",
        "ggwgebieden_volgnummer",
        "ggwgebieden_identificatie",
        "bestaat_uit_buurten_identificatie",
        "bestaat_uit_buurten_volgnummer",
        "begin_geldigheid",
        "eind_geldigheid",
    }

    assert records[0] == {
        "id": 1,
        "ggwgebieden_id": "03630950000000.1",
        "bestaat_uit_buurten_id": "03630023753960.1",
        "ggwgebieden_identificatie": "03630950000000",
        "ggwgebieden_volgnummer": 1,
        "bestaat_uit_buurten_identificatie": "03630023753960",
        "bestaat_uit_buurten_volgnummer": 1,
        "begin_geldigheid": datetime.date(2019, 1, 12),
        "eind_geldigheid": None,
    }

    assert set(records[0].keys()) == columns


def test_ndjson_import_nm_composite_selfreferencing_keys(
    here, engine, kadastraleobjecten_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "kadastraleobjecten.ndjson"
    importer = NDJSONImporter(kadastraleobjecten_schema, engine)
    importer.generate_db_objects("kadastraleobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    records = [dict(r) for r in engine.execute("SELECT * from brk_kadastraleobjecten")]
    assert len(records) == 2
    # An "id" should have been generated, concat of the composite key fields
    assert "id" in records[0]
    assert records[0]["id"] == "KAD.001.1"
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT * from brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject"
        )
    ]
    assert len(records) == 1
    assert sorted((n, v) for n, v in records[0].items()) == (
        [
            ("id", 1),
            ("is_ontstaan_uit_kadastraalobject_id", "KAD.002.1"),
            ("is_ontstaan_uit_kadastraalobject_identificatie", "KAD.002"),
            ("is_ontstaan_uit_kadastraalobject_volgnummer", 1),
            ("kadastraleobjecten_id", "KAD.001.1"),
            ("kadastraleobjecten_identificatie", "KAD.001"),
            ("kadastraleobjecten_volgnummer", 1),
        ]
    )


def test_ndjson_import_nested_tables(here, engine, verblijfsobjecten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "verblijfsobjecten.ndjson"
    importer = NDJSONImporter(verblijfsobjecten_schema, engine)
    importer.generate_db_objects("verblijfsobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [
        dict(r)
        for r in engine.execute(
            "SELECT code, omschrijving, parent_id FROM baggob_verblijfsobjecten_gebruiksdoel"
        )
    ]
    assert len(records) == 2
    assert sorted((n, v) for n, v in records[0].items()) == (
        [
            ("code", "1"),
            ("omschrijving", "doel 1"),
            ("parent_id", "VB.1"),
        ]
    )


def test_ndjson_import_1n(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from meetbouten_meetbouten")]
    assert len(records) == 1
    # The foreign key, needed by Django, should be there
    assert "ligt_in_buurt_id" in records[0]
    # And should have the concatenated value
    assert records[0]["ligt_in_buurt_id"] == "10180001.1"
    # Should have a field identificatie
    assert "ligt_in_buurt_identificatie" in records[0]


def test_inactive_relation_that_are_commented_out(here, engine, stadsdelen_schema, dbsession):
    """Prove that relations that are commented out in the schema are flattened to strings"""
    ndjson_path = here / "files" / "data" / "stadsdelen.ndjson"
    importer = NDJSONImporter(stadsdelen_schema, engine)
    importer.generate_db_objects("stadsdelen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from gebieden_stadsdelen ORDER BY id")]
    # Field is stringified, because in schema the relation is 'disabled'
    assert records[0]["ligt_in_gemeente"] == '{"identificatie": "0363"}'


def test_missing_fields_in_jsonpath_provenance(here, engine, woonplaatsen_schema, dbsession):
    """Prove that missing fields in jsonpath provenance fields do not crash"""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from baggob_woonplaatsen ORDER BY id")]
    assert len(records) == 2
    assert records[1]["status_code"] is None


def test_ndjson_import_with_shortnames_in_schema(
    here, engine, hr_schema, verblijfsobjecten_schema, dbsession
):
    """Prove that data for schemas with shortnames for tables/fields is imported correctly."""
    ndjson_path = here / "files" / "data" / "hr.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects(
        "maatschappelijkeactiviteiten", truncate=True, ind_extra_index=False
    )
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from hr_activiteiten")]
    assert len(records) == 1
    assert records[0]["kvknummer"] == "90004213"
    assert records[0]["gevestigd_in_identificatie"] == "01002"
    assert records[0]["gevestigd_in_volgnummer"] == 3
    assert records[0]["gevestigd_in_id"] == "01002.3"

    records = [
        dict(r) for r in engine.execute("SELECT * from hr_activiteiten_sbi_maatschappelijk")
    ]
    assert len(records) == 1
    assert records[0] == {"parent_id": "90004213", "bronwaarde": 1130, "id": 1}

    records = [
        dict(r) for r in engine.execute("SELECT * from hr_activiteiten_sbi_voor_activiteit")
    ]
    assert len(records) == 1
    # In this case, the through table does not have extra fields, because the
    # FK to the target is not a composite key.
    assert records[0] == {
        "id": 1,
        "activiteiten_id": "90004213",
        "sbi_voor_activiteit_id": "01131",
    }

    records = [dict(r) for r in engine.execute("SELECT * from hr_activiteiten_verblijfsobjecten")]
    assert len(records) == 1
    assert records[0] == {
        "id": 1,
        "activiteiten_id": "90004213",
        "verblijfsobjecten_id": "01001.1",
        "verblijfsobjecten_identificatie": "01001",
        "verblijfsobjecten_volgnummer": 1,
    }


def test_ndjson_import_with_shortnames_in_identifier(here, engine, hr_schema, dbsession):
    """Prove that data for schemas with shortnames in an identifier is imported correctly."""
    ndjson_path = here / "files" / "data" / "hr_sbi_act_nr.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects("sbiactiviteiten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    records = [
        dict(r) for r in engine.execute("SELECT * from hr_sbiactiviteiten ORDER BY sbi_act_nr")
    ]
    assert records == [{"sbi_act_nr": "12"}, {"sbi_act_nr": "16"}]


def test_provenance_for_schema_field_ids_equal_to_ndjson_keys(
    here, engine, woonplaatsen_schema, dbsession
):
    """Prove that imports where the schema field is equal to the key in the imported ndjson
    data are processed correctly."""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    records = [dict(r) for r in engine.execute("SELECT * from baggob_woonplaatsen ORDER BY id")]
    assert len(records) == 2
    assert records[0]["heeft_dossier_id"] == "GV12"
    assert records[1]["heeft_dossier_id"] is None
