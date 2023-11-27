"""Event tests."""
import json
from datetime import date, datetime

import orjson
import pytest
from more_itertools import peekable

from schematools.events.full import EventsProcessor

# pytestmark = pytest.mark.skip("all tests disabled")


def _load_events_from_file_as_full_load_sequence(importer, events_path):
    with open(events_path, "rb") as ef:
        is_first = True
        p = peekable(ef)

        for line in p:
            event_id, event_meta_str, data_str = line.split(b"|", maxsplit=2)
            event_meta = orjson.loads(event_meta_str)
            event_meta |= {
                "full_load_sequence": True,
                "first_of_sequence": is_first,
                "last_of_sequence": p.peek(None) is None,
            }
            event_data = orjson.loads(data_str)
            importer.process_event(
                event_meta,
                event_data,
            )
            is_first = False


def test_event_process_insert(
    here, db_schema, tconn, local_metadata, gebieden_schema, benk_schema
):
    """Prove that event gets inserted."""
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_bouwblokken")]
    assert len(records) == 2
    assert records[0]["code"] == "AA01"
    assert records[1]["code"] == "AA02"
    assert records[0]["eind_geldigheid"] is None
    assert records[0]["begin_geldigheid"] == date(2006, 6, 12)


def test_event_process_insert_object(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema
):
    """Prove that event gets inserted correctly with object split in columns."""
    events_path = here / "files" / "data" / "peilmerken.gobevents"
    importer = EventsProcessor(
        [nap_schema, gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert records[0]["status_code"] == 3
    assert records[0]["status_omschrijving"] == "Vervallen"


def test_event_process_update(here, tconn, local_metadata, gebieden_schema):
    """Prove that event gets updated."""
    events_path = here / "files" / "data" / "bouwblokken_update.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"
    assert records[0]["begin_geldigheid"] == date(2020, 2, 5)
    assert records[0]["registratiedatum"] == datetime(2020, 2, 5, 15, 6, 43)


def test_event_process_delete(here, tconn, local_metadata, gebieden_schema):
    """Prove that event gets deleted."""
    events_path = here / "files" / "data" / "bouwblokken_delete.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"


def test_event_process_nm_relation_delete(here, tconn, local_metadata, gebieden_schema, salogger):
    """Prove that NM relations of an event get deleted."""
    events_path = (
        here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten_delete.gobevents"
    )
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [
        dict(r) for r in tconn.execute("SELECT * FROM gebieden_ggwgebieden_bestaat_uit_buurten")
    ]
    assert len(records) == 0


def test_event_process_relation_update_parent_table(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema
):
    events_path = here / "files" / "data" / "peilmerken.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]

    # Imported objects without relations
    assert len(records) == 1
    assert records[0]["ligt_in_bouwblok_id"] is None
    assert records[0]["ligt_in_bouwblok_identificatie"] is None
    assert records[0]["ligt_in_bouwblok_volgnummer"] is None

    events_path = here / "files" / "data" / "peilmerken_ligt_in_bouwblok.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    rel_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_ligt_in_bouwblok")]
    parent_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]

    # Should have inserted the rel and updated relation columns in  parent (object) table
    assert len(rel_records) == 1
    assert len(parent_records) == 1

    assert parent_records[0]["ligt_in_bouwblok_id"] == "03630012095746.1"
    assert parent_records[0]["ligt_in_bouwblok_identificatie"] == "03630012095746"
    assert parent_records[0]["ligt_in_bouwblok_volgnummer"] == 1

    events_path = here / "files" / "data" / "peilmerken_ligt_in_bouwblok.delete.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    rel_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_ligt_in_bouwblok")]
    parent_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]

    # Rel table row should be deleted, fields in parent table should be set to None again
    assert len(rel_records) == 0
    assert len(parent_records) == 1
    assert records[0]["ligt_in_bouwblok_id"] is None
    assert records[0]["ligt_in_bouwblok_identificatie"] is None
    assert records[0]["ligt_in_bouwblok_volgnummer"] is None


def test_event_process_relation_skip_update_parent_table_nm_relations(
    here, db_schema, tconn, local_metadata, gebieden_schema
):
    events_path = here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten.gobevents"
    importer = EventsProcessor([gebieden_schema], tconn, local_metadata=local_metadata)

    # First import row in parent table
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_ggwgebieden")]
    assert len(records) == 1

    # Then import row in relation table and check that parent table is not updated and no error is
    # raised
    events_path = (
        here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten_reltable_add.gobevents"
    )
    importer.load_events_from_file(events_path)
    records = [
        dict(r) for r in tconn.execute("SELECT * FROM gebieden_ggwgebieden_bestaat_uit_buurten")
    ]
    assert len(records) == 1
    assert records[0]["ggwgebieden_id"] == "03630950000000.1"
    assert records[0]["bestaat_uit_buurten_id"] == "03630023754008ADD.1"

    # Update row in relation table and check that parent table is not updated and no error is
    # raised
    events_path = (
        here
        / "files"
        / "data"
        / "gebieden_ggwgebieden_bestaat_uit_buurten_reltable_modify.gobevents"
    )
    importer.load_events_from_file(events_path)
    records = [
        dict(r) for r in tconn.execute("SELECT * FROM gebieden_ggwgebieden_bestaat_uit_buurten")
    ]
    assert len(records) == 1
    assert records[0]["ggwgebieden_id"] == "03630950000000.1"
    assert records[0]["bestaat_uit_buurten_id"] == "03630023754008MOD.1"


def test_event_process_relation_update_parent_table_shortname(
    here, db_schema, tconn, local_metadata, gebieden_schema
):
    """Tests updating of the parent table with a relation attribute with a shortname."""

    # Import bouwblokken in object table
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    importer = EventsProcessor([gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_bouwblokken")]
    assert len(records) == 2

    # Import relation table row event for relation with shortname
    events_path = here / "files" / "data" / "bouwblokken_ligt_in_buurt_met_te_lange_naam.gobevents"
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM gebieden_bouwblokken")]
    assert len(records) == 2

    record = [r for r in records if r["identificatie"] == "03630012096976"][0]
    assert record["lgt_in_brt_id"] == "03630023754008ADD.1"
    assert record["lgt_in_brt_identificatie"] == "03630023754008ADD"
    assert record["lgt_in_brt_volgnummer"] == 1


def test_events_process_relation_without_table_update_parent_table(
    here, db_schema, tconn, local_metadata, brk_schema_without_bag_relations
):
    # First, verify that the relation table indeed does not exist
    res = next(
        tconn.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename  = 'brk_tenaamstellingen_van_kadastraalsubject');"
        )
    )
    assert res[0] is False

    importer = EventsProcessor(
        [brk_schema_without_bag_relations], tconn, local_metadata=local_metadata
    )

    # Import relation vanKadastraalsubject. Has no table, but we want to update the parent table
    events_path = here / "files" / "data" / "tenaamstellingen.gobevents"
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_tenaamstellingen")]
    assert len(records) == 2
    assert [(r["identificatie"], r["van_kadastraalsubject_id"]) for r in records] == [
        ("NL.IMKAD.Tenaamstelling.ajdkfl4j4", "NL.IMKAD.Persoon.1124ji44kd"),
        ("NL.IMKAD.Tenaamstelling.adkfkadfkjld", "NL.IMKAD.Persoon.2f4802kkdd"),
    ]

    # Test that parent table is updated
    events_path = here / "files" / "data" / "tenaamstellingen_van_kadastraalsubject.gobevents"
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_tenaamstellingen")]
    assert len(records) == 2
    assert [(r["identificatie"], r["van_kadastraalsubject_id"]) for r in records] == [
        ("NL.IMKAD.Tenaamstelling.ajdkfl4j4", "NL.IMKAD.Persoon.20042004eeeeefjd"),
        ("NL.IMKAD.Tenaamstelling.adkfkadfkjld", None),
    ]


def test_events_process_relation_without_table_update_parent_table_full_load(
    here, db_schema, tconn, local_metadata, brk_schema_without_bag_relations
):
    # First, verify that the relation table indeed does not exist
    res = next(
        tconn.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename  = 'brk_tenaamstellingen_van_kadastraalsubject');"
        )
    )
    assert res[0] is False

    importer = EventsProcessor(
        [brk_schema_without_bag_relations], tconn, local_metadata=local_metadata
    )

    # Import relation vanKadastraalsubject. Has no table, but we want to update the parent table
    events_path = here / "files" / "data" / "tenaamstellingen.gobevents"
    _load_events_from_file_as_full_load_sequence(importer, events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_tenaamstellingen")]
    assert len(records) == 2
    assert [(r["identificatie"], r["van_kadastraalsubject_id"]) for r in records] == [
        ("NL.IMKAD.Tenaamstelling.ajdkfl4j4", "NL.IMKAD.Persoon.1124ji44kd"),
        ("NL.IMKAD.Tenaamstelling.adkfkadfkjld", "NL.IMKAD.Persoon.2f4802kkdd"),
    ]

    # Test that parent table is updated
    events = [
        (
            {
                "event_type": "ADD",
                "event_id": 1,
                "dataset_id": "brk",
                "table_id": "tenaamstellingen_vanKadastraalsubject",
                "full_load_sequence": True,
                "first_of_sequence": True,
                "last_of_sequence": False,
            },
            {
                "id": 1,
                "tenaamstellingen_id": "NL.IMKAD.Tenaamstelling.ajdkfl4j4.4",
                "tenaamstellingen_identificatie": "NL.IMKAD.Tenaamstelling.ajdkfl4j4",
                "tenaamstellingen_volgnummer": 4,
                "van_kadastraalsubject_id": "NL.IMKAD.Persoon.20042004eeeeefjd",
                "van_kadastraalsubject_identificatie": "NL.IMKAD.Persoon.20042004eeeeefjd",
            },
        ),
        (
            {
                "event_type": "ADD",
                "event_id": 2,
                "dataset_id": "brk",
                "table_id": "tenaamstellingen_vanKadastraalsubject",
                "full_load_sequence": True,
                "first_of_sequence": False,
                "last_of_sequence": True,
            },
            {
                "id": 3,
                "tenaamstellingen_id": "NL.IMKAD.Tenaamstelling.ajdkeeeefad.4",
                "tenaamstellingen_identificatie": "NL.IMKAD.Tenaamstelling.ajdkeeeefad",
                "tenaamstellingen_volgnummer": 4,
                "van_kadastraalsubject_id": "NL.IMKAD.Persoon.20042004eeeeefjd",
                "van_kadastraalsubject_identificatie": "NL.IMKAD.Persoon.20042004eeeeefjd",
            },
        ),
    ]

    importer.process_events(events, recovery_mode=False)
    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_tenaamstellingen")]
    assert len(records) == 2

    # Full load does not update parent table, so result should be the same as above. This
    # testcase is included though to make sure we don't get an error on the missing relation table.
    assert [(r["identificatie"], r["van_kadastraalsubject_id"]) for r in records] == [
        ("NL.IMKAD.Tenaamstelling.ajdkfl4j4", "NL.IMKAD.Persoon.1124ji44kd"),
        ("NL.IMKAD.Tenaamstelling.adkfkadfkjld", "NL.IMKAD.Persoon.2f4802kkdd"),
    ]


def test_event_process_full_load_sequence(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema
):
    """Test consists of three parts:

    - First, load usual peilmerken events from peilmerken.gobevents.
    - Then, load events from peilmerken_full_load_sequence_start.gobevents. This should
      create the tmp table and not change the active table.
    - Lastly, load events from peilmerken_full_load_sequence_end.gobevents. This should
      end the full load sequence and
      replace the active table with the tmp table. Also, the tmp table should be removed.

    The peilmerken identificaties in the *.gobevents files used are unique, so we can
    check that the objects are in the expected tables.
    """

    def load_events(events_file):
        events_path = here / "files" / "data" / events_file
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], tconn, local_metadata=local_metadata
        )
        importer.load_events_from_file_using_bulk(events_path)

    # 1.
    load_events("peilmerken.gobevents")
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert len(records) == 1
    assert records[0]["identificatie"] == "70780001"

    # 2.
    load_events("peilmerken_full_load_sequence_start.gobevents")
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert len(records) == 1
    assert records[0]["identificatie"] == "70780001"

    records = [dict(r) for r in tconn.execute("SELECT * FROM nap.nap_peilmerken_full_load")]
    assert len(records) == 3
    assert records[0]["identificatie"] == "70780002"
    assert records[1]["identificatie"] == "70780003"
    assert records[2]["identificatie"] == "70780004"

    # 3.
    load_events("peilmerken_full_load_sequence_end.gobevents")
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert len(records) == 4
    assert records[0]["identificatie"] == "70780002"
    assert records[1]["identificatie"] == "70780003"
    assert records[2]["identificatie"] == "70780004"
    assert records[3]["identificatie"] == "70780005"

    res = next(
        tconn.execute(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename  = 'nap_peilmerken_full_load');"
        )
    )
    assert res[0] is False


def test_event_process_geometry_attrs(
    here, db_schema, tconn, local_metadata, brk_schema_without_bag_relations
):
    events_path = here / "files" / "data" / "kadastraleobjecten_geometry.gobevents"

    importer = EventsProcessor(
        [brk_schema_without_bag_relations], tconn, local_metadata=local_metadata
    )
    importer.load_events_from_file_using_bulk(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_kadastraleobjecten")]

    assert len(records) == 1
    record = records[0]
    assert record["bijpijling_geometrie"] is not None
    assert record["geometrie"] is not None


def _create_peilmerken_event(id: str, jaar: int, type: str = "ADD", **kwargs) -> tuple[dict, dict]:
    """Creates a peilmerken event. Use 'jaar' to make sure the event is unique."""

    return (
        {
            "event_type": type,
            "dataset_id": "nap",
            "table_id": "peilmerken",
            **kwargs,
        },
        {
            "identificatie": id,
            "hoogte_tov_nap": -2.6954,
            "jaar": jaar,
            "merk": {
                "code": "7",
                "omschrijving": "Bijzondere merktekens bijvoorbeeld zeskantige bout, "
                "stalen pen etc.",
            },
            "omschrijving": "Gemaal aan de Ringvaart gelegen aan de Schipholweg. Vervallen?",
            "windrichting": "Z",
            "x_coordinaat_muurvlak": 84,
            "y_coordinaat_muurvlak": 37,
            "rws_nummer": "25D0039",
            "geometrie": "01010000204071000000000000C05FFC400000000050601D41",
            "status": {"code": 3, "omschrijving": "Vervallen"},
            "vervaldatum": "2018-04-23",
            "ligt_in_bouwblok_id": None,
            "ligt_in_bouwblok_identificatie": None,
            "ligt_in_bouwblok_volgnummer": None,
            "publiceerbaar": False,
        },
    )


def _assert_have_peilmerken(
    id_years: list[tuple[str, int]], tconn, check_full_load_table: bool = False
):
    tablename = "nap_peilmerken" if not check_full_load_table else "nap.nap_peilmerken_full_load"
    records = [dict(r) for r in tconn.execute(f"SELECT * FROM {tablename}")]  # noqa: S608
    assert len(records) == len(id_years)

    for record, (id, year) in zip(records, id_years):
        assert record["identificatie"] == id
        assert record["jaar"] == year


def _import_assert_result_expect_exception(
    importer, events, expected_result, recovery_mode=False, check_full_load_table=False
):
    with pytest.raises(Exception):
        importer.process_events(events, recovery_mode)
    _assert_have_peilmerken(expected_result, importer.conn, check_full_load_table)


def _import_assert_result(
    importer, events, expected_result, recovery_mode=False, check_full_load_table=False
):
    importer.process_events(events, recovery_mode)
    _assert_have_peilmerken(expected_result, importer.conn, check_full_load_table)


def test_event_process_recovery_regular(
    db_schema, engine, local_metadata, nap_schema, gebieden_schema
):
    """Tests adding of regular events (that are not part of a full load sequence) with
    and without recovery mode enabled.

    Initialise database with some rows.

    Try to add rows:
    - 1. One that already exists, with recovery_mode = False. Should raise an error.
    - 2. One that doesn't exist, with recovery_mode = False. Should be added.
    - 3. One that already exists, with recovery_mode = True. Should be ignored.
    - 4. One that doesn't exist, with recovery_mode = True. Should be added.
    """
    with engine.connect() as conn:
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], conn, local_metadata=local_metadata
        )

        # Init
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2018, event_id=1),
                _create_peilmerken_event("2", 2019, event_id=2),
            ],
            [("1", 2018), ("2", 2019)],
        )

        # 1. One that already exists, with recovery_mode = False. Should raise an error.
        _import_assert_result_expect_exception(
            importer, [_create_peilmerken_event("1", 2017, event_id=3)], [("1", 2018), ("2", 2019)]
        )

        # 2. One that doesn't exist, with recovery_mode = False. Should be added.
        _import_assert_result(
            importer,
            [_create_peilmerken_event("3", 2020, event_id=4)],
            [("1", 2018), ("2", 2019), ("3", 2020)],
        )

        # 3. One that already exists, with recovery_mode = True. Should be ignored.
        _import_assert_result(
            importer,
            [_create_peilmerken_event("3", 2019, event_id=5)],
            [("1", 2018), ("2", 2019), ("3", 2020)],
            recovery_mode=True,
        )

        # 4. One that doesn't exist, with recovery_mode = True. Should be added.
        _import_assert_result(
            importer,
            [_create_peilmerken_event("4", 2021, event_id=6)],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            recovery_mode=True,
        )
        conn.close()


def test_event_process_recovery_full_load_first(
    db_schema, engine, local_metadata, nap_schema, gebieden_schema
):
    """Tests adding of events that are part of a full load sequence and where the first
    event is the first in the sequence.

    Initialise full load with some rows.

    Try to add multiple rows, where the first one:
    - Already exists, with recovery_mode = False. New rows should be added.
    - Doesn't exist, with recovery_mode = False. New rows should be added.
    - Already exists, with recovery_mode = True. New rows should be added.
    - Doesn't exist, with recovery_mode = True. New rows should be added.

    Note: When the first event to handle in this case is the first in the sequence, the
    _full_load table is always truncated. This means that in every case we replace the
    table.

    """
    with engine.connect() as conn:
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], conn, local_metadata=local_metadata
        )

        # Init
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "1", 2018, full_load_sequence=True, first_of_sequence=True, event_id=1
                ),
                _create_peilmerken_event("2", 2019, full_load_sequence=True, event_id=2),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 1. Already exists, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "1", 2020, full_load_sequence=True, first_of_sequence=True, event_id=3
                ),
                _create_peilmerken_event("2", 2021, full_load_sequence=True, event_id=4),
            ],
            [("1", 2020), ("2", 2021)],
            check_full_load_table=True,
        )

        # 2. Doesn't exist, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "3", 2020, full_load_sequence=True, first_of_sequence=True, event_id=5
                ),
                _create_peilmerken_event("4", 2021, full_load_sequence=True, event_id=6),
            ],
            [("3", 2020), ("4", 2021)],
            check_full_load_table=True,
        )

        # 3. Already exists, with recovery_mode = True. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "3", 2022, full_load_sequence=True, first_of_sequence=True, event_id=7
                ),
                _create_peilmerken_event("4", 2023, full_load_sequence=True, event_id=8),
            ],
            [("3", 2022), ("4", 2023)],
            check_full_load_table=True,
            recovery_mode=True,
        )

        # 4. Doesn't exist, with recovery_mode = True. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "5", 2020, full_load_sequence=True, first_of_sequence=True, event_id=9
                ),
                _create_peilmerken_event("6", 2021, full_load_sequence=True, event_id=10),
            ],
            [("5", 2020), ("6", 2021)],
            check_full_load_table=True,
            recovery_mode=True,
        )


def test_event_process_recovery_full_load_no_first_no_last(
    db_schema, engine, local_metadata, nap_schema, gebieden_schema
):
    """Tests adding of events that are part of a full load sequence and where events to
    be added are neither the first nor the last in the sequence.

    Initialise full load with some rows.

    Try to add multiple rows, where the first one:
    - Already exists, with recovery_mode = False. Should raise an error.
    - Doesn't exist, with recovery_mode = False. New rows should be added.
    - Already exists, with recovery_mode = True. Rows should be ignored.
    - Doesn't exist, with recovery_mode = True. New rows should be added.

    """
    with engine.connect() as conn:
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], conn, local_metadata=local_metadata
        )

        # Init
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "1", 2018, full_load_sequence=True, first_of_sequence=True, event_id=1
                ),
                _create_peilmerken_event("2", 2019, full_load_sequence=True, event_id=2),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 1. Already exists, with recovery_mode = False. Should raise an error.
        _import_assert_result_expect_exception(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True, event_id=3),
                _create_peilmerken_event("2", 2023, full_load_sequence=True, event_id=4),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 2. Doesn't exist, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("3", 2020, full_load_sequence=True, event_id=5),
                _create_peilmerken_event("4", 2021, full_load_sequence=True, event_id=6),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            check_full_load_table=True,
        )

        # 3. Already exists, with recovery_mode = True. Rows should be ignored.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True, event_id=7),
                _create_peilmerken_event("2", 2023, full_load_sequence=True, event_id=8),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            check_full_load_table=True,
            recovery_mode=True,
        )

        # 4. Doesn't exist, with recovery_mode = True. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("5", 2022, full_load_sequence=True, event_id=9),
                _create_peilmerken_event("6", 2023, full_load_sequence=True, event_id=10),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021), ("5", 2022), ("6", 2023)],
            check_full_load_table=True,
            recovery_mode=True,
        )


def test_event_process_recovery_full_load_last_table_empty(
    db_schema, engine, local_metadata, nap_schema, gebieden_schema
):
    """Tests adding of events that are part of a full load sequence and where the last
    event is the last in the sequence and the _full_load table is empty.

    No full load initialisation; table should be empty

    Try to add multiple rows, where the first one:
    - Doesn't exist, with recovery_mode = False. New rows should be added and object table
      replaced.
    - Doesn't exist, with recovery_mode = True. Everything in this message should be ignored

    """
    with engine.connect() as conn:
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], conn, local_metadata=local_metadata
        )

        def _init_empty_full_load_table(importer):
            # Init and truncate right after to get inconsistent state
            _import_assert_result(
                importer,
                [
                    _create_peilmerken_event(
                        "1", 2018, full_load_sequence=True, first_of_sequence=True, event_id=1
                    ),
                    _create_peilmerken_event("2", 2019, full_load_sequence=True, event_id=2),
                ],
                [("1", 2018), ("2", 2019)],
                check_full_load_table=True,
            )
            importer.conn.execute("TRUNCATE TABLE nap.nap_peilmerken_full_load")

        # 1. Doesn't exist, with recovery_mode = False. New rows should be added and object
        #    table replaced.
        _init_empty_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2018, full_load_sequence=True, event_id=3),
                _create_peilmerken_event(
                    "2", 2019, full_load_sequence=True, last_of_sequence=True, event_id=4
                ),
            ],
            [
                ("1", 2018),
                ("2", 2019),
            ],
        )

        # 2. Doesn't exist, with recovery_mode = True. Everything in this message should
        #    be ignored
        _init_empty_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("3", 2018, full_load_sequence=True, event_id=5),
                _create_peilmerken_event(
                    "4", 2019, full_load_sequence=True, last_of_sequence=True, event_id=6
                ),
            ],
            [
                ("1", 2018),
                ("2", 2019),
            ],
            recovery_mode=True,
        )


def test_event_process_recovery_full_load_last_table_not_empty(
    db_schema, engine, local_metadata, nap_schema, gebieden_schema
):
    """Tests adding of events that are part of a full load sequence and where the last event is
    the last in the sequence and the _full_load table is not empty.

    Initialise full load with some rows.

    Try to add multiple rows, where the first one:
    - Already exists, with recovery_mode = False. Should raise an error.
    - Doesn't exist, with recovery_mode = False. New rows should be added and object table
      replaced.
    - Already exists, with recovery_mode = True. Rows should be ignored. Object table replaced.
    - Doesn't exist, with recovery_mode = True. Rows should be added. Object table replaced.

    """
    with engine.connect() as conn:
        importer = EventsProcessor(
            [nap_schema, gebieden_schema], conn, local_metadata=local_metadata
        )

        def _init_full_load_table(importer):
            for table in ["nap_peilmerken_full_load", "benk_lasteventids"]:
                try:
                    conn.execute(f"TRUNCATE TABLE {table}")  # noqa: S608
                except Exception:  # noqa: S110
                    pass
            importer.lasteventids.clear_cache()

            _import_assert_result(
                importer,
                [
                    _create_peilmerken_event(
                        "1", 2018, full_load_sequence=True, first_of_sequence=True, event_id=1
                    ),
                    _create_peilmerken_event("2", 2019, full_load_sequence=True, event_id=2),
                ],
                [("1", 2018), ("2", 2019)],
                check_full_load_table=True,
            )

        # 1. Already exists, with recovery_mode = False. Should raise an error.
        _init_full_load_table(importer)
        _import_assert_result_expect_exception(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True, event_id=3),
                _create_peilmerken_event(
                    "2", 2023, full_load_sequence=True, last_of_sequence=True, event_id=4
                ),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 2. Doesn't exist, with recovery_mode = False. New rows should be added and
        #    object table replaced.
        _init_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("3", 2020, full_load_sequence=True, event_id=5),
                _create_peilmerken_event(
                    "4", 2021, full_load_sequence=True, last_of_sequence=True, event_id=6
                ),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
        )

        # 3. Already exists, with recovery_mode = True. Rows should be ignored. Object
        #    table replaced.
        _init_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2023, full_load_sequence=True, event_id=7),
                _create_peilmerken_event(
                    "2", 2024, full_load_sequence=True, last_of_sequence=True, event_id=8
                ),
            ],
            [("1", 2018), ("2", 2019)],
            recovery_mode=True,
        )

        # 4. Doesn't exist, with recovery_mode = True. Rows should be added. Object table
        #    replaced.
        _init_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("3", 2020, full_load_sequence=True, event_id=9),
                _create_peilmerken_event(
                    "4", 2021, full_load_sequence=True, last_of_sequence=True, event_id=10
                ),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            recovery_mode=True,
        )


def test_event_process_last_event_id(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema, benk_schema
):
    def get_last_event_id(tablename: str = "nap_peilmerken"):
        res = tconn.execute(
            f"SELECT last_event_id FROM benk_lasteventids "  # noqa: S608  # nosec: B608
            f"WHERE \"table\"='{tablename}'"
        ).fetchone()

        return res[0] if res is not None else None

    importer = EventsProcessor(
        [nap_schema, gebieden_schema, benk_schema], tconn, local_metadata=local_metadata
    )

    # 1. Assert start state
    assert get_last_event_id() is None

    events = [
        _create_peilmerken_event("1", 2018, event_id=203),
        _create_peilmerken_event("2", 2019, event_id=210),
    ]
    importer.process_events(events)

    # 2. Add rows and assert they exist
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert [2018, 2019] == [r["jaar"] for r in records]
    assert get_last_event_id() == 210

    events = [
        _create_peilmerken_event("2", 2020, type="MODIFY", event_id=211),
    ]
    importer.process_events(events)

    # 3. Assert event with newer ID is applied
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert [2018, 2020] == [r["jaar"] for r in records]
    assert get_last_event_id() == 211

    events = [
        _create_peilmerken_event("1", 2021, type="MODIFY", event_id=204),
        _create_peilmerken_event("2", 2021, type="MODIFY", event_id=211),
    ]
    importer.process_events(events)

    # 4. Assert event with older IDs are ignored
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert [2018, 2020] == [r["jaar"] for r in records]
    assert get_last_event_id() == 211


def test_event_process_last_event_id_full_load_sequence(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema, benk_schema
):
    def get_last_event_id(tablename: str = "nap_peilmerken"):
        res = tconn.execute(
            f"SELECT last_event_id FROM benk_lasteventids "  # noqa: S608
            f"WHERE \"table\"='{tablename}'"  # noqa: S608
        ).fetchone()

        return res[0] if res is not None else None

    importer = EventsProcessor(
        [nap_schema, gebieden_schema, benk_schema], tconn, local_metadata=local_metadata
    )

    # 1. Assert start state
    assert get_last_event_id("nap_peilmerken") is None
    assert get_last_event_id("nap_peilmerken_full_load") is None

    events = [
        _create_peilmerken_event(
            "1", 2018, event_id=203, full_load_sequence=True, first_of_sequence=True
        ),
        _create_peilmerken_event("2", 2019, event_id=210, full_load_seuqence=True),
    ]
    importer.process_events(events)

    # 2. Add rows and assert they exist
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap.nap_peilmerken_full_load")]
    assert [2018, 2019] == [r["jaar"] for r in records]
    assert get_last_event_id("nap_peilmerken") is None
    assert get_last_event_id("nap_peilmerken_full_load") == 210

    events = [
        _create_peilmerken_event("3", 2020, type="ADD", event_id=212, full_load_sequence=True),
    ]
    importer.process_events(events)

    # 3. Assert event with newer ID is applied
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap.nap_peilmerken_full_load")]
    assert [2018, 2019, 2020] == [r["jaar"] for r in records]
    assert get_last_event_id("nap_peilmerken") is None
    assert get_last_event_id("nap_peilmerken_full_load") == 212

    events = [
        _create_peilmerken_event("4", 2021, type="ADD", event_id=204, full_load_sequence=True),
        _create_peilmerken_event("5", 2021, type="ADD", event_id=211, full_load_sequence=True),
    ]
    importer.process_events(events)

    # 4. Assert event with older IDs are ignored
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap.nap_peilmerken_full_load")]
    assert [2018, 2019, 2020] == [r["jaar"] for r in records]
    assert get_last_event_id("nap_peilmerken") is None
    assert get_last_event_id("nap_peilmerken_full_load") == 212

    # 5. End full load. Table should be replaced and last_event_id copied to main table and reset
    events = [
        _create_peilmerken_event("4", 2021, type="ADD", event_id=213, full_load_sequence=True),
        _create_peilmerken_event(
            "5", 2022, type="ADD", event_id=217, full_load_sequence=True, last_of_sequence=True
        ),
    ]
    importer.process_events(events)

    # 4. Assert event with older IDs are ignored
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]
    assert [2018, 2019, 2020, 2021, 2022] == [r["jaar"] for r in records]
    assert get_last_event_id("nap_peilmerken") == 217
    assert get_last_event_id("nap_peilmerken_full_load") is None


def test_events_process_full_load_sequence_snake_cased_schema(
    here, db_schema, tconn, local_metadata, brk2_simple_schema
):
    """Tests whether the correct (snake_cased) schema for brk2 is used for the full load."""

    event_meta = {
        "event_type": "ADD",
        "event_id": 1,
        "dataset_id": "brk2",
        "table_id": "gemeentes",
        "full_load_sequence": True,
        "first_of_sequence": True,
    }
    event_data = {"identificatie": "0363", "naam": "Amsterdam"}

    importer = EventsProcessor([brk2_simple_schema], tconn, local_metadata=local_metadata)
    importer.process_event(event_meta, event_data)

    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_2.brk_2_gemeentes_full_load")]
    assert len(records) == 1
    assert records[0]["identificatie"] == "0363"
    assert records[0]["naam"] == "Amsterdam"

    event_meta = {
        "event_type": "ADD",
        "event_id": 2,
        "dataset_id": "brk2",
        "table_id": "gemeentes",
        "full_load_sequence": True,
        "last_of_sequence": True,
    }
    event_data = {
        "identificatie": "0457",
        "naam": "Weesp",
    }
    importer.process_event(event_meta, event_data)

    records = [dict(r) for r in tconn.execute("SELECT * FROM brk_2_gemeentes")]
    assert len(records) == 2
    assert records[0]["identificatie"] == "0363"
    assert records[0]["naam"] == "Amsterdam"
    assert records[1]["identificatie"] == "0457"
    assert records[1]["naam"] == "Weesp"


def test_events_process_full_load_relation_update_parent_table(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema
):
    events_path = here / "files" / "data" / "peilmerken.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]

    # Imported objects without relations
    assert len(records) == 1
    assert records[0]["ligt_in_bouwblok_id"] is None
    assert records[0]["ligt_in_bouwblok_identificatie"] is None
    assert records[0]["ligt_in_bouwblok_volgnummer"] is None

    event_meta = {
        "event_type": "ADD",
        "event_id": 1,
        "dataset_id": "nap",
        "table_id": "peilmerken_ligtInBouwblok",
        "full_load_sequence": True,
        "first_of_sequence": True,
        "last_of_sequence": True,
    }
    event_data = {
        "id": 1,
        "peilmerken_id": "70780001",
        "peilmerken_identificatie": "70780001",
        "ligt_in_bouwblok_id": "03630012095746.1",
        "ligt_in_bouwblok_identificatie": "03630012095746",
        "ligt_in_bouwblok_volgnummer": 1,
    }

    importer.process_event(event_meta, event_data)

    rel_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_ligt_in_bouwblok")]
    parent_records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken")]

    # Should have updated relation columns in  parent (object) table
    assert len(rel_records) == 1
    assert len(parent_records) == 1

    assert parent_records[0]["ligt_in_bouwblok_id"] == "03630012095746.1"
    assert parent_records[0]["ligt_in_bouwblok_identificatie"] == "03630012095746"
    assert parent_records[0]["ligt_in_bouwblok_volgnummer"] == 1


def load_json_results_file(location):
    class _JSONDecoder(json.JSONDecoder):
        """Custom JSON decoder that converts date(time) strings to date(time) objects."""

        def __init__(self, *args, **kwargs):
            json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

        def object_hook(self, obj):
            ret = {}
            for key, value in obj.items():
                if key in ("begin_geldigheid", "eind_geldigheid") and value is not None:
                    if len(value) == 10:
                        ret[key] = date.fromisoformat(value)
                    else:
                        ret[key] = datetime.fromisoformat(value)
                else:
                    ret[key] = value
            return ret

    with open(location) as f:
        return json.load(f, cls=_JSONDecoder)


def delete_id(d: dict):
    return {k: v for k, v in d.items() if k != "id"}


def delete_ids(lst: list[dict]):
    return [delete_id(d) for d in lst]


def assert_results(tconn, expected_results: dict, testname: str):
    for table_name, expected_result in expected_results.items():
        records = [dict(r) for r in tconn.execute(f"SELECT * FROM {table_name}")]  # noqa: S608
        records = delete_ids(records)

        assert len(records) == len(
            expected_result
        ), f"Number of records in {table_name} does not match for test {testname}"
        for res in expected_result:
            assert res in records, f"Record {res} not found in {table_name} for test {testname}"
        for rec in records:
            assert (
                rec in expected_result
            ), f"Unexpected record {rec} found in {table_name} for test {testname}"


def test_events_nested_table(here, db_schema, tconn, local_metadata, bag_verblijfsobjecten_schema):
    expected_results = load_json_results_file(
        here / "files" / "data" / "expect" / "events_nested_table.json"
    )

    importer = EventsProcessor(
        [bag_verblijfsobjecten_schema], tconn, local_metadata=local_metadata
    )

    # Load initial data with nested objects
    events_path = here / "files" / "data" / "verblijfsobjecten.gobevents"
    importer.load_events_from_file(events_path)
    assert_results(tconn, expected_results["initial_add"], "Load initial data")

    # Modify nested objects
    events_path = here / "files" / "data" / "verblijfsobjecten.modify_nested.gobevents"
    importer.load_events_from_file(events_path)
    assert_results(tconn, expected_results["modify_nested"], "Modify nested objects")

    # Empty nested objects
    events_path = here / "files" / "data" / "verblijfsobjecten.empty_nested.gobevents"
    importer.load_events_from_file(events_path)
    assert_results(tconn, expected_results["empty_nested"], "Remove nested objects")

    # Modify nested objects again
    events_path = here / "files" / "data" / "verblijfsobjecten.modify_nested_2.gobevents"
    importer.load_events_from_file(events_path)
    assert_results(tconn, expected_results["modify_nested"], "Modify nested objects again")

    # Delete full object
    events_path = here / "files" / "data" / "verblijfsobjecten.delete.gobevents"
    importer.load_events_from_file(events_path)
    assert_results(tconn, expected_results["delete"], "Delete everything")

    # Now test full load. Full load only works with ADD events, so we reuse the ADD event from
    # above and add the full load metadata.
    events_path = here / "files" / "data" / "verblijfsobjecten.gobevents"
    _load_events_from_file_as_full_load_sequence(importer, events_path)

    assert_results(tconn, expected_results["initial_add"], "Load initial data using full load")


def test_full_load_shortnames(here, db_schema, tconn, local_metadata, hr_simple_schema):
    importer = EventsProcessor([hr_simple_schema], tconn, local_metadata=local_metadata)

    # First import an object with nested objects
    events = [
        (
            {
                "event_type": "ADD",
                "event_id": 1,
                "dataset_id": "hr",
                "table_id": "maatschappelijkeactiviteiten",
                "full_load_sequence": True,
                "first_of_sequence": True,
                "last_of_sequence": True,
            },
            {
                "kvknummer": 42,
                "email_adressen": [
                    {
                        "email_adres": "address1@example.com",
                    },
                    {
                        "email_adres": "address2@example.com",
                    },
                ],
            },
        )
    ]
    importer.process_events(events)

    # Not testing contents here, but merely the fact that the right tables are used without errors
    records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac")]
    assert len(records) == 1
    assert records[0]["heeft_hoofdvestiging_id"] is None

    nested_records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac_email_adressen")]
    assert len(nested_records) == 2
    assert nested_records[0]["parent_id"] == "42"
    assert nested_records[0]["email_adres"] == "address1@example.com"

    # Now test adding a relation object that references a parent table with short name
    events = [
        (
            {
                "dataset_id": "hr",
                "table_id": "maatschappelijkeactiviteiten_heeftHoofdvestiging",
                "event_type": "ADD",
                "event_id": 1658565091,
                "tid": "42.AMSBI.24902480",
                "generated_timestamp": "2023-10-05T09:59:05.314873",
                "full_load_sequence": True,
                "first_of_sequence": True,
                "last_of_sequence": True,
            },
            {
                "mac_kvknummer": "42",
                "mac_id": "42",
                "heeft_hoofdvestiging_vestigingsnummer": "24902480",
                "heeft_hoofdvestiging_id": "24902480",
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "id": 457172,
            },
        )
    ]

    importer.process_events(events)
    rel_records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac_heeft_hoofdvestiging")]
    assert len(rel_records) == 1
    assert rel_records[0]["id"] == 457172
    assert rel_records[0]["mac_id"] == "42"
    assert rel_records[0]["heeft_hoofdvestiging_id"] == "24902480"

    records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac")]
    assert len(records) == 1
    assert records[0]["heeft_hoofdvestiging_id"] == "24902480"


def test_full_load_shortnames_update(here, db_schema, tconn, local_metadata, hr_simple_schema):
    importer = EventsProcessor([hr_simple_schema], tconn, local_metadata=local_metadata)

    # First import an object with nested objects
    events = [
        (
            {
                "event_type": "ADD",
                "event_id": 1,
                "dataset_id": "hr",
                "table_id": "maatschappelijkeactiviteiten",
            },
            {
                "kvknummer": 42,
                "email_adressen": [
                    {
                        "email_adres": "address1@example.com",
                    },
                    {
                        "email_adres": "address2@example.com",
                    },
                ],
            },
        )
    ]
    importer.process_events(events)

    # Not testing contents here, but merely the fact that the right tables are used without errors
    records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac")]
    assert len(records) == 1
    assert records[0]["heeft_hoofdvestiging_id"] is None

    nested_records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac_email_adressen")]
    assert len(nested_records) == 2
    assert nested_records[0]["parent_id"] == "42"
    assert nested_records[0]["email_adres"] == "address1@example.com"

    # Now test adding a relation object that references a parent table with short name
    events = [
        (
            {
                "dataset_id": "hr",
                "table_id": "maatschappelijkeactiviteiten_heeftHoofdvestiging",
                "event_type": "ADD",
                "event_id": 1658565091,
                "tid": "42.AMSBI.24902480",
                "generated_timestamp": "2023-10-05T09:59:05.314873",
            },
            {
                "mac_kvknummer": "42",
                "mac_id": "42",
                "heeft_hoofdvestiging_vestigingsnummer": "24902480",
                "heeft_hoofdvestiging_id": "24902480",
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "id": 457172,
            },
        )
    ]

    importer.process_events(events)
    rel_records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac_heeft_hoofdvestiging")]
    assert len(rel_records) == 1
    assert rel_records[0]["id"] == 457172
    assert rel_records[0]["mac_id"] == "42"
    assert rel_records[0]["heeft_hoofdvestiging_id"] == "24902480"

    records = [dict(r) for r in tconn.execute("SELECT * FROM hr_mac")]
    assert len(records) == 1
    assert records[0]["heeft_hoofdvestiging_id"] == "24902480"


def test_reset_lasteventid_after_incomplete_full_load(
    here, db_schema, tconn, local_metadata, nap_schema, gebieden_schema
):
    """This testcase tests whether the lasteventid is reset after an incomplete full load sequence.
    This should not happen during normal usage, but can happen when the full load stream is
    manually removed from the queue.
    """
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    events = [
        _create_peilmerken_event(
            "1", 2018, event_id=3, full_load_sequence=True, first_of_sequence=True
        ),
        _create_peilmerken_event(
            "2", 2018, event_id=4, full_load_sequence=True, first_of_sequence=False
        ),
    ]

    importer.process_events(events)
    lasteventrecord = tconn.execute(
        "SELECT * FROM benk_lasteventids WHERE \"table\" = 'nap_peilmerken_full_load'"
    ).fetchone()
    assert lasteventrecord["last_event_id"] == 4

    events = [
        _create_peilmerken_event(
            "1", 2018, event_id=1, full_load_sequence=True, first_of_sequence=True
        ),
    ]
    importer.process_events(events)
    lasteventrecord = tconn.execute(
        "SELECT * FROM benk_lasteventids WHERE \"table\" = 'nap_peilmerken_full_load'"
    ).fetchone()
    assert lasteventrecord["last_event_id"] == 1


def test_avoid_duplicate_key_after_full_load(
    here, db_schema, tconn, local_metadata, bag_verblijfsobjecten_schema
):
    """Make sure we don't get duplicate key errors after a full load sequence with a serial id
    field in the table."""

    def create_event(gebruiksdoel_cnt: int, event_id: int, identificatie: str, **extra_headers):
        gebruiksdoelen = [
            {"code": i, "omschrijving": f"doel {i}"} for i in range(1, gebruiksdoel_cnt + 1)
        ]
        return (
            {
                "event_type": "ADD",
                "event_id": event_id,
                "dataset_id": "bag",
                "table_id": "verblijfsobjecten",
                **extra_headers,
            },
            {
                "identificatie": identificatie,
                "volgnummer": 1,
                "gebruiksdoel": gebruiksdoelen,
                "toegang": None,
                "ligt_in_buurt": {},
                "begin_geldigheid": "2018-10-22T00:00:00.000000",
                "eind_geldigheid": None,
            },
        )

    importer = EventsProcessor(
        [bag_verblijfsobjecten_schema], tconn, local_metadata=local_metadata
    )

    # Add objects with in total 4 nested objects
    full_load_events = [
        create_event(2, 1, "VB1", full_load_sequence=True, first_of_sequence=True),
        create_event(2, 2, "VB2", full_load_sequence=True, last_of_sequence=True),
    ]
    importer.process_events(full_load_events)

    update_event = [create_event(1, 3, "VB3")]
    importer.process_events(update_event)
