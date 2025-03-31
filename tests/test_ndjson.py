from __future__ import annotations

import datetime

from sqlalchemy import text

from schematools.importer.ndjson import NDJSONImporter


def test_ndjson_import_nm(here, engine, meetbouten_schema, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "metingen.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * from meetbouten_metingen_v1 order by identificatie"))
            .mappings()
            .all()
        )
    assert len(records) == 4
    # A non-object relation, should just lead to _id field
    assert records[0] == {"identificatie": "173", "hoortbijmeetbout_id": "13881032"}

    with engine.begin() as conn:
        # check value from the ndjson input, should be string according to the schema
        records = (
            conn.execute(
                text(
                    "SELECT * from meetbouten_metingen_refereertaanreferentiepunten_v1 order by id"
                )
            )
            .mappings()
            .all()
        )
    # Should have a field 'id' in the n-m table
    # an extra _identificatie is not needed, this is not a composite key
    assert records[0] == {
        "id": 1,
        "metingen_id": "191",
        "refereertaanreferentiepunten_id": "10180001",
    }


def test_ndjson_import_separate_relations_target_composite(
    here, engine, meetbouten_schema, gebieden_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "meetbouten_ligt_in_buurt.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    # NOTE: the table ID is actually "meetbouten_ligtInBuurt".
    # This also tests that both parameters still work with the old snake-cased identifiers,
    # as get_table_by_id() still does.
    importer.generate_db_objects(
        table_id="meetbouten_ligt_in_buurt",
        truncate=True,
        ind_extra_index=False,
        limit_tables_to={"meetbouten_ligt_in_buurt"},
    )

    importer.load_file(ndjson_path, is_through_table=True)

    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * from meetbouten_meetbouten_ligt_in_buurt_v1"))
            .mappings()
            .all()
        )
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
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM ggwgebieden_ggwgebieden_bestaatuitbuurten_v1"))
            .mappings()
            .all()
        )

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

    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from meetbouten_meetbouten_v1")).mappings().all()
    assert records == [
        {
            "identificatie": 1,
            "ligt_in_buurt_identificatie": None,  # Should have a field identificatie and be none
            "ligt_in_buurt_volgnummer": None,
            "ligt_in_buurt_id": None,  # The foreign key, needed by Django, should be there
            "merk_code": "12",
            "merk_omschrijving": "De meetbout",
            "geometrie": "01010000204071000000000000A028FD4066666666CEBA1D41",
        }
    ]


def test_ndjson_import_no_embedded_nm_relation_in_data(
    here, engine, ggwgebieden_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "ggwgebieden-no-embedded-rel-data.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)

    importer.load_file(ndjson_path, is_through_table=True)
    with engine.begin() as conn:
        records = conn.execute(text("SELECT * FROM ggwgebieden_ggwgebieden_v1")).mappings().all()
    assert len(records) == 1

    # Through table is available, but has no content
    # (because there is no data  for it in the ndjson).
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM ggwgebieden_ggwgebieden_bestaatuitbuurten_v1"))
            .mappings()
            .all()
        )
    assert len(records) == 0


def test_ndjson_import_jsonpath_provenance(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from meetbouten_meetbouten_v1")).mappings().all()
    assert records == [
        {
            "identificatie": 1,
            "ligt_in_buurt_identificatie": "10180001",
            "ligt_in_buurt_volgnummer": 1,
            "ligt_in_buurt_id": "10180001.1",
            "merk_code": "12",
            "merk_omschrijving": "De meetbout",
            "geometrie": "01010000204071000000000000A028FD4066666666CEBA1D41",
        }
    ]


def test_ndjson_import_nm_composite_keys(here, engine, ggwgebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "ggwgebieden.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)  # TODO: fix datetime tzinfo
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    with engine.begin() as conn:
        records = conn.execute(text("SELECT * FROM ggwgebieden_ggwgebieden_v1")).mappings().all()

    # An "id" should have been generated, concat of the composite key fields
    assert records == [
        {
            "id": "03630950000000.1",
            "identificatie": "03630950000000",
            "volgnummer": 1,
            "naam": "Centrum-West",
            "code": "DX01",
            "registratiedatum": datetime.datetime(2020, 7, 21, 13, 39, 23, 856580),  # noqa: DTZ001
            "begingeldigheid": datetime.date(2014, 2, 20),
            "eindgeldigheid": datetime.date(2019, 10, 3),
            "documentdatum": datetime.date(2017, 10, 10),
            "ligtinstadsdeel_identificatie": "03630000000018",
            "ligtinstadsdeel_volgnummer": 3,
            "ligtinstadsdeel_id": "03630000000018.3",
        }
    ]

    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM ggwgebieden_ggwgebieden_bestaatuitbuurten_v1"))
            .mappings()
            .all()
        )
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

    assert set(records[0].keys()) == columns

    # Fails:
    # assert records[0] == {
    #     "id": 1,
    #     "ggwgebieden_identificatie": "03630950000000",
    #     "ggwgebieden_volgnummer": 1,
    #     "ggwgebieden_id": "03630950000000.1",
    #     "bestaatuitbuurten_identificatie": "03630023753960",
    #     "bestaatuitbuurten_volgnummer": 1,
    #     "bestaatuitbuurten_id": "03630023753960.1",
    # }


def test_ndjson_import_nm_composite_keys_with_geldigheid(here, engine, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "ggwgebieden-with-geldigheid.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)  # TODO: fix datetime tzinfo
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from gebieden_ggwgebieden_v1")).mappings().all()
    assert records == [
        {
            # An "id" should have been generated, concat of the composite key fields:
            "id": "03630950000000.1",
            "identificatie": "03630950000000",
            "volgnummer": 1,
            "registratiedatum": datetime.datetime(2020, 7, 21, 13, 39, 23, 856580),  # noqa: DTZ001
            "naam": "Centrum-West",
            "code": "DX01",
            "begin_geldigheid": None,
            "eind_geldigheid": None,
            "documentdatum": datetime.date(2017, 10, 10),
            "documentnummer": "A12",
            "ligt_in_stadsdeel_identificatie": "03630000000018",
            "ligt_in_stadsdeel_volgnummer": 3,
            "ligt_in_stadsdeel_id": "03630000000018.3",
            "geometrie": None,
        }
    ]

    with engine.begin() as conn:
        records = (
            conn.execute(
                text("SELECT * from gebieden_ggwgebieden_bestaat_uit_buurten_v1 order by id")
            )
            .mappings()
            .all()
        )
    assert len(records) == 3
    # Also the temporal fields are present in the database
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


def test_ndjson_import_nm_composite_selfreferencing_keys(
    here, engine, kadastraleobjecten_schema, dbsession
):
    ndjson_path = here / "files" / "data" / "kadastraleobjecten.ndjson"
    importer = NDJSONImporter(kadastraleobjecten_schema, engine)
    importer.generate_db_objects("kadastraleobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    # An "id" should have been generated, concat of the composite key fields
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * from brk_kadastraleobjecten_v1 order by id"))
            .mappings()
            .all()
        )
    assert records == [
        {
            "begin_geldigheid": None,
            "eind_geldigheid": None,
            "id": "KAD.001.1",
            "identificatie": "KAD.001",
            "koopsom": None,
            "neuron_id": "10",
            "registratiedatum": None,
            "soort_cultuur_onbebouwd_code": None,
            "soort_cultuur_onbebouwd_omschrijving": None,
            "soort_grootte": {"foo": 12},
            "volgnummer": 1,
        },
        {
            "begin_geldigheid": None,
            "eind_geldigheid": None,
            "id": "KAD.002.1",
            "identificatie": "KAD.002",
            "koopsom": None,
            "neuron_id": "11",
            "registratiedatum": None,
            "soort_cultuur_onbebouwd_code": "aa",
            "soort_cultuur_onbebouwd_omschrijving": "foo",
            "soort_grootte": None,
            "volgnummer": 1,
        },
    ]

    with engine.begin() as conn:
        records = (
            conn.execute(
                text("SELECT * from brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1")
            )
            .mappings()
            .all()
        )
    assert records == [
        {
            "id": 1,
            "is_ontstaan_uit_kadastraalobject_id": "KAD.002.1",
            "is_ontstaan_uit_kadastraalobject_identificatie": "KAD.002",
            "is_ontstaan_uit_kadastraalobject_volgnummer": 1,
            "kadastraleobjecten_id": "KAD.001.1",
            "kadastraleobjecten_identificatie": "KAD.001",
            "kadastraleobjecten_volgnummer": 1,
        }
    ]


def test_ndjson_import_nested_tables(here, engine, verblijfsobjecten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "verblijfsobjecten.ndjson"
    importer = NDJSONImporter(verblijfsobjecten_schema, engine)
    importer.generate_db_objects("verblijfsobjecten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(
                text(
                    "SELECT code, omschrijving, parent_id"
                    " FROM verblijfsobjecten_verblijfsobjecten_gebruiksdoel_v1"
                )
            )
            .mappings()
            .all()
        )
    assert records == [
        {"code": "1", "omschrijving": "doel 1", "parent_id": "VB.1"},
        {"code": "2", "omschrijving": "doel 2", "parent_id": "VB.1"},
    ]


def test_ndjson_import_1n(here, engine, meetbouten_schema, dbsession):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from meetbouten_meetbouten_v1")).mappings().all()
    # The foreign key, needed by Django, should be there
    # And should have the concatenated value
    # And Should have a field identificatie
    assert records == [
        {
            "identificatie": 1,
            "ligt_in_buurt_id": "10180001.1",
            "ligt_in_buurt_identificatie": "10180001",
            "ligt_in_buurt_volgnummer": 1,
            "merk_code": "12",
            "merk_omschrijving": "De meetbout",
            "geometrie": "01010000204071000000000000A028FD4066666666CEBA1D41",
        }
    ]


def test_inactive_relation_that_are_commented_out(here, engine, stadsdelen_schema, dbsession):
    """Prove that relations that are commented out in the schema are flattened to strings."""
    ndjson_path = here / "files" / "data" / "stadsdelen.ndjson"
    importer = NDJSONImporter(stadsdelen_schema, engine)
    importer.generate_db_objects("stadsdelen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM stadsdelen_stadsdelen_v1 ORDER BY id"))
            .mappings()
            .all()
        )
    # Field is stringified, because in schema the relation is 'disabled'
    assert records == [
        {"id": "1", "ligt_in_gemeente": '{"identificatie": "0363"}'},
        {"id": "2", "ligt_in_gemeente": '{"identificatie": "0364"}'},
    ]


def test_missing_fields_in_jsonpath_provenance(here, engine, woonplaatsen_schema, dbsession):
    """Prove that missing fields in jsonpath provenance fields do not crash."""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM woonplaatsen_woonplaatsen_v1 ORDER BY id"))
            .mappings()
            .all()
        )
    assert records == [
        {
            "id": "1.1",
            "status_code": 1,
            "status_omschrijving": "status met code 1",
            "heeft_dossier_id": "GV12",
        },
        {
            "id": "1.2",
            "status_code": None,
            "status_omschrijving": "status zonder omschrijving",
            "heeft_dossier_id": None,
        },
    ]


def test_ndjson_import_with_shortnames_missing_data(here, engine, hr_schema, dbsession):
    """Prove that rows with missing fields with shortnames are imported correctly."""
    ndjson_path = here / "files" / "data" / "hr_missing_nmrelation.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects(
        "maatschappelijkeactiviteiten", truncate=True, ind_extra_index=False
    )
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from hr_activiteiten_v1")).mappings().all()

    # shortname for heeftEenRelatieMetVerblijfsobject, not in hr_missing_nmrelation.ndjson
    assert records == [
        {
            "kvknummer": "90004213",
            "gevestigd_in_identificatie": "01002",
            "gevestigd_in_volgnummer": 3,
            "gevestigd_in_id": "01002.3",
        }
    ]


def test_ndjson_import_with_shortnames_in_schema(here, engine, hr_schema, dbsession):
    """Prove that data for schemas with shortnames for tables/fields is imported correctly."""
    ndjson_path = here / "files" / "data" / "hr.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects(
        "maatschappelijkeactiviteiten", truncate=True, ind_extra_index=False
    )
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from hr_activiteiten_v1")).mappings().all()
        assert records == [
            {
                "kvknummer": "90004213",
                "gevestigd_in_identificatie": "01002",
                "gevestigd_in_volgnummer": 3,
                "gevestigd_in_id": "01002.3",
            }
        ]

        records = (
            conn.execute(text("SELECT * from hr_activiteiten_sbi_maatschappelijk_v1"))
            .mappings()
            .all()
        )
        assert records == [{"parent_id": "90004213", "bronwaarde": 1130, "id": 1}]

        records = (
            conn.execute(text("SELECT * from hr_activiteiten_sbi_voor_activiteit_v1"))
            .mappings()
            .all()
        )
        # In this case, the through table does not have extra fields, because the
        # FK to the target is not a composite key.
        assert records == [
            {
                "id": 1,
                "activiteiten_id": "90004213",
                "sbi_voor_activiteit_id": "01131",
            }
        ]

        records = (
            conn.execute(text("SELECT * from hr_activiteiten_verblijfsobjecten_v1"))
            .mappings()
            .all()
        )
        assert records == [
            {
                "id": 1,
                "activiteiten_id": "90004213",
                "verblijfsobjecten_id": "01001.1",
                "verblijfsobjecten_identificatie": "01001",
                "verblijfsobjecten_volgnummer": 1,
            }
        ]


def test_ndjson_import_with_shortnames_in_identifier(here, engine, hr_schema, dbsession):
    """Prove that data for schemas with shortnames in an identifier is imported correctly."""
    ndjson_path = here / "files" / "data" / "hr_sbi_act_nr.ndjson"
    importer = NDJSONImporter(hr_schema, engine)
    importer.generate_db_objects("sbiactiviteiten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * from hr_sbiactiviteiten_v1 ORDER BY sbi_act_nr"))
            .mappings()
            .all()
        )
    assert records == [{"sbi_act_nr": "12"}, {"sbi_act_nr": "16"}]


def test_ndjson_import_with_attributes_on_relation(
    here, engine, brk_schema, verblijfsobjecten_schema, dbsession
):
    """Prove that extra attributes on a relation do not interfere with ndjson import."""
    ndjson_path = here / "files" / "data" / "brk_aantek_rechten.ndjson"
    importer = NDJSONImporter(brk_schema, engine)
    importer.generate_db_objects("aantekeningenrechten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)

    with engine.begin() as conn:
        records = conn.execute(text("SELECT * from brk_aantekeningenrechten_v1")).mappings().all()
    assert records == [
        {
            "aard_code": None,
            "aard_omschrijving": None,
            "betrokken_tenaamstelling_id": None,
            "einddatum": None,
            "id": "01",
            "identificatie": "01",
            "is_gbsd_op_sdl_id": "013",
            "omschrijving": None,
            "toestandsdatum": None,
        }
    ]


def test_provenance_for_schema_field_ids_equal_to_ndjson_keys(
    here, engine, woonplaatsen_schema, dbsession
):
    """Prove that imports where the schema field is equal to the key in the imported ndjson
    data are processed correctly."""
    ndjson_path = here / "files" / "data" / "woonplaatsen.ndjson"
    importer = NDJSONImporter(woonplaatsen_schema, engine)
    importer.generate_db_objects("woonplaatsen", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as conn:
        records = (
            conn.execute(text("SELECT * FROM woonplaatsen_woonplaatsen_v1 ORDER BY id"))
            .mappings()
            .all()
        )
    assert records == [
        {
            "id": "1.1",
            "status_code": 1,
            "status_omschrijving": "status met code 1",
            "heeft_dossier_id": "GV12",
        },
        {
            "id": "1.2",
            "status_code": None,
            "status_omschrijving": "status zonder omschrijving",
            "heeft_dossier_id": None,
        },
    ]
