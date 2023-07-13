"""Event tests."""
from datetime import date, datetime

import pytest

from schematools.events.full import EventsProcessor

# pytestmark = pytest.mark.skip("all tests disabled")


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

    records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_full_load")]
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
    tablename = "nap_peilmerken" if not check_full_load_table else "nap_peilmerken_full_load"
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
                    "1", 2018, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("2", 2019, full_load_sequence=True),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 1. Already exists, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "1", 2020, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("2", 2021, full_load_sequence=True),
            ],
            [("1", 2020), ("2", 2021)],
            check_full_load_table=True,
        )

        # 2. Doesn't exist, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "3", 2020, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("4", 2021, full_load_sequence=True),
            ],
            [("3", 2020), ("4", 2021)],
            check_full_load_table=True,
        )

        # 3. Already exists, with recovery_mode = True. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event(
                    "3", 2022, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("4", 2023, full_load_sequence=True),
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
                    "5", 2020, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("6", 2021, full_load_sequence=True),
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
                    "1", 2018, full_load_sequence=True, first_of_sequence=True
                ),
                _create_peilmerken_event("2", 2019, full_load_sequence=True),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 1. Already exists, with recovery_mode = False. Should raise an error.
        _import_assert_result_expect_exception(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True),
                _create_peilmerken_event("2", 2023, full_load_sequence=True),
            ],
            [("1", 2018), ("2", 2019)],
            check_full_load_table=True,
        )

        # 2. Doesn't exist, with recovery_mode = False. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("3", 2020, full_load_sequence=True),
                _create_peilmerken_event("4", 2021, full_load_sequence=True),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            check_full_load_table=True,
        )

        # 3. Already exists, with recovery_mode = True. Rows should be ignored.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True),
                _create_peilmerken_event("2", 2023, full_load_sequence=True),
            ],
            [("1", 2018), ("2", 2019), ("3", 2020), ("4", 2021)],
            check_full_load_table=True,
            recovery_mode=True,
        )

        # 4. Doesn't exist, with recovery_mode = True. New rows should be added.
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("5", 2022, full_load_sequence=True),
                _create_peilmerken_event("6", 2023, full_load_sequence=True),
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
                        "1", 2018, full_load_sequence=True, first_of_sequence=True
                    ),
                    _create_peilmerken_event("2", 2019, full_load_sequence=True),
                ],
                [("1", 2018), ("2", 2019)],
                check_full_load_table=True,
            )
            importer.conn.execute("TRUNCATE TABLE nap_peilmerken_full_load")

        # 1. Doesn't exist, with recovery_mode = False. New rows should be added and object
        #    table replaced.
        _init_empty_full_load_table(importer)
        _import_assert_result(
            importer,
            [
                _create_peilmerken_event("1", 2018, full_load_sequence=True),
                _create_peilmerken_event(
                    "2", 2019, full_load_sequence=True, last_of_sequence=True
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
                _create_peilmerken_event("3", 2018, full_load_sequence=True),
                _create_peilmerken_event(
                    "4", 2019, full_load_sequence=True, last_of_sequence=True
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
            try:
                importer.conn.execute("TRUNCATE TABLE nap_peilmerken_full_load")
            except Exception:  # noqa: S110
                pass

            _import_assert_result(
                importer,
                [
                    _create_peilmerken_event(
                        "1", 2018, full_load_sequence=True, first_of_sequence=True
                    ),
                    _create_peilmerken_event("2", 2019, full_load_sequence=True),
                ],
                [("1", 2018), ("2", 2019)],
                check_full_load_table=True,
            )

        # 1. Already exists, with recovery_mode = False. Should raise an error.
        _init_full_load_table(importer)
        _import_assert_result_expect_exception(
            importer,
            [
                _create_peilmerken_event("1", 2022, full_load_sequence=True),
                _create_peilmerken_event(
                    "2", 2023, full_load_sequence=True, last_of_sequence=True
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
                _create_peilmerken_event("3", 2020, full_load_sequence=True),
                _create_peilmerken_event(
                    "4", 2021, full_load_sequence=True, last_of_sequence=True
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
                _create_peilmerken_event("1", 2023, full_load_sequence=True),
                _create_peilmerken_event(
                    "2", 2024, full_load_sequence=True, last_of_sequence=True
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
                _create_peilmerken_event("3", 2020, full_load_sequence=True),
                _create_peilmerken_event(
                    "4", 2021, full_load_sequence=True, last_of_sequence=True
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


# TODO replace 'recovery mode' in full load with this logic later, but first make sure it
# really solves our problem, because 'recovery mode' works and we don't want to replace is
# with something that may not work as well.
# def test_event_process_last_event_id_full_load_sequence(here, db_schema, tconn, local_metadata,
#       nap_schema, gebieden_schema, benk_schema):
#     def get_last_event_id(tablename: str = 'nap_peilmerken'):
#         res = tconn.execute(f"SELECT last_event_id FROM benk_lasteventids
#         WHERE \"table\"='{tablename}'").fetchone()
#
#         return res[0] if res is not None else None
#
#     importer = EventsProcessor(
#         [nap_schema, gebieden_schema, benk_schema], tconn, local_metadata=local_metadata
#     )
#
#     # 1. Assert start state
#     assert get_last_event_id("nap_peilmerken") is None
#     assert get_last_event_id("nap_peilmerken_full_load") is None
#
#     events = [
#         _create_peilmerken_event("1", 2018, event_id=203, full_load_sequence=True,
#         first_of_sequence=True),
#         _create_peilmerken_event("2", 2019, event_id=210, full_load_seuqence=True),
#     ]
#     importer.process_events(events)
#
#     # 2. Add rows and assert they exist
#     records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_full_load")]
#     assert [2018, 2019] == [r["jaar"] for r in records]
#     assert get_last_event_id("nap_peilmerken") is None
#     assert get_last_event_id("nap_peilmerken_full_load") == 210
#
#     events = [
#         _create_peilmerken_event("3", 2020, type="ADD", event_id=212),
#     ]
#     importer.process_events(events)
#
#     # 3. Assert event with newer ID is applied
#     records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_full_load")]
#     assert [2018, 2019, 2020] == [r["jaar"] for r in records]
#     assert get_last_event_id("nap_peilmerken") is None
#     assert get_last_event_id("nap_peilmerken_full_load") == 212
#
#     events = [
#         _create_peilmerken_event("4", 2021, type="ADD", event_id=204),
#         _create_peilmerken_event("5", 2021, type="ADD", event_id=211),
#     ]
#     importer.process_events(events)
#
#     # 4. Assert event with older IDs are ignored
#     records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_full_load")]
#     assert [2018, 2019, 2020] == [r["jaar"] for r in records]
#     assert get_last_event_id("nap_peilmerken") is None
#     assert get_last_event_id("nap_peilmerken_full_load") == 212
#
#     # 5. End full load. Table should be replaced and last_event_id copied to main table and reset
#     events = [
#         _create_peilmerken_event("4", 2021, type="ADD", event_id=213),
#         _create_peilmerken_event("5", 2022, type="ADD", event_id=217),
#     ]
#     importer.process_events(events)
#
#     # 4. Assert event with older IDs are ignored
#     records = [dict(r) for r in tconn.execute("SELECT * FROM nap_peilmerken_full_load")]
#     assert [2018, 2019, 2020, 2021, 2022] == [r["jaar"] for r in records]
#     assert get_last_event_id("nap_peilmerken") == 217
#     assert get_last_event_id("nap_peilmerken_full_load") is None
