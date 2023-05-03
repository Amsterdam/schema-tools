"""Event tests."""
from datetime import date, datetime

from schematools.events.full import EventsProcessor

# pytestmark = pytest.mark.skip("all tests disabled")


def test_event_process_insert(here, db_schema, tconn, local_metadata, gebieden_schema):
    """Prove that event gets inserted."""
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
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
    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]
    assert records[0]["status_code"] == 3
    assert records[0]["status_omschrijving"] == "Vervallen"


def test_event_process_update(here, tconn, local_metadata, gebieden_schema):
    """Prove that event gets updated."""
    events_path = here / "files" / "data" / "bouwblokken_update.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
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
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
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
    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]

    # Imported objects without relations
    assert len(records) == 1
    assert records[0]["ligt_in_bouwblok_id"] is None
    assert records[0]["ligt_in_bouwblok_identificatie"] is None
    assert records[0]["ligt_in_bouwblok_volgnummer"] is None

    events_path = here / "files" / "data" / "peilmerken_ligt_in_bouwblok.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    rel_records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken_ligt_in_bouwblok")]
    parent_records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]

    # Should have inserted the rel and updated relation columns in  parent (object) table
    assert len(rel_records) == 1
    assert len(parent_records) == 1

    assert parent_records[0]["ligt_in_bouwblok_id"] == "03630012095746.1"
    assert parent_records[0]["ligt_in_bouwblok_identificatie"] == "03630012095746"
    assert parent_records[0]["ligt_in_bouwblok_volgnummer"] == 1

    events_path = here / "files" / "data" / "peilmerken_ligt_in_bouwblok.delete.gobevents"
    importer = EventsProcessor([nap_schema, gebieden_schema], tconn, local_metadata=local_metadata)
    importer.load_events_from_file_using_bulk(events_path)
    rel_records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken_ligt_in_bouwblok")]
    parent_records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]

    # Rel table row should be deleted, fields in parent table should be set to None again
    assert len(rel_records) == 0
    assert len(parent_records) == 1
    assert records[0]["ligt_in_bouwblok_id"] is None
    assert records[0]["ligt_in_bouwblok_identificatie"] is None
    assert records[0]["ligt_in_bouwblok_volgnummer"] is None


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
    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]
    assert len(records) == 1
    assert records[0]["identificatie"] == "70780001"

    # 2.
    load_events("peilmerken_full_load_sequence_start.gobevents")
    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]
    assert len(records) == 1
    assert records[0]["identificatie"] == "70780001"

    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken_full_load")]
    assert len(records) == 3
    assert records[0]["identificatie"] == "70780002"
    assert records[1]["identificatie"] == "70780003"
    assert records[2]["identificatie"] == "70780004"

    # 3.
    load_events("peilmerken_full_load_sequence_end.gobevents")
    records = [dict(r) for r in tconn.execute("SELECT * from nap_peilmerken")]
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
