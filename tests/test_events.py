from datetime import datetime, date
from schematools.events import EventsProcessor


def test_event_process_insert(here, tconn, bouwblokken_schema):
    # def test_event_process_insert(here, engine, bouwblokken_schema, dbsession):
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    bouwblokken_table = bouwblokken_schema.get_table_by_id("bouwblokken")
    importer = EventsProcessor(
        bouwblokken_table, 28992, tconn, local_metadata=True, truncate=True
    )
    importer.load_events_from_file(events_path)
    with tconn as conn:
        records = [dict(r) for r in conn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 2
    assert records[0]["code"] == "AA01"
    assert records[1]["code"] == "AA02"
    assert records[0]["eind_geldigheid"] is None
    assert records[0]["begin_geldigheid"] == date(2006, 6, 12)


def test_event_process_update(here, tconn, transaction, bouwblokken_schema):
    # def test_event_process_update(here, engine, bouwblokken_schema, dbsession):
    events_path = here / "files" / "data" / "bouwblokken_update.gobevents"
    bouwblokken_table = bouwblokken_schema.get_table_by_id("bouwblokken")
    importer = EventsProcessor(
        bouwblokken_table, 28992, tconn, local_metadata=True, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"
    assert records[0]["begin_geldigheid"] == date(2020, 2, 5)
    assert records[0]["registratiedatum"] == datetime(2020, 2, 5, 15, 6, 43)


def test_event_process_delete(here, tconn, transaction, bouwblokken_schema):
    # def test_event_process_delete(here, engine, bouwblokken_schema, dbsession):
    events_path = here / "files" / "data" / "bouwblokken_delete.gobevents"
    bouwblokken_table = bouwblokken_schema.get_table_by_id("bouwblokken")
    importer = EventsProcessor(
        bouwblokken_table, 28992, tconn, local_metadata=True, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"
