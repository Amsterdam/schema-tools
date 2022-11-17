"""Event tests."""
from datetime import date, datetime

from dateutil.parser import parse as dtparse

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


def test_event_process_1n_relation_insert(here, tconn, local_metadata, gebieden_schema):
    """Prove that 1-N relation gets imported correctly."""
    events_bb_path = here / "files" / "data" / "gebieden_bouwblokken_ligt_in_buurt.gobevents"

    importer = EventsProcessor(
        [gebieden_schema],
        tconn,
        local_metadata=local_metadata,
        truncate=True,
    )
    importer.load_events_from_file(events_bb_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["ligt_in_buurt_id"] == "03630000000707.2"

    # The FK relation also has a junction table, to store extra information
    # about the relation
    through_records = [
        dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken_ligt_in_buurt")
    ]

    assert len(through_records) == 1
    assert through_records[0]["ligt_in_buurt_id"] == "03630000000707.2"

    available_columns = {
        "id",
        "bouwblokken_id",
        "ligt_in_buurt_id",
        "bouwblokken_identificatie",
        "bouwblokken_volgnummer",
        "ligt_in_buurt_identificatie",
        "ligt_in_buurt_volgnummer",
        "begin_geldigheid",
        "eind_geldigheid",
    }

    assert set(through_records[0].keys()) == available_columns


def test_event_process_1n_relation_delete(here, tconn, local_metadata, gebieden_schema):
    """Prove that 1-N relation gets deleted by modifying the embedded relation."""
    events_path = here / "files" / "data" / "gebieden_bouwblokken_ligt_in_buurt_delete.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["ligt_in_buurt_identificatie"] is None
    assert records[0]["ligt_in_buurt_volgnummer"] is None
    assert records[0]["ligt_in_buurt_id"] is None


def test_event_process_nm_relation_insert(
    here,
    tconn,
    local_metadata,
    gebieden_schema,
):
    """Prove that NM relations of an event get inserted."""
    events_path = here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [
        dict(r) for r in tconn.execute("SELECT * FROM gebieden_ggwgebieden_bestaat_uit_buurten")
    ]
    assert len(records) == 1
    assert records[0]["bestaat_uit_buurten_id"] == "03630023754008.1"
    assert records[0]["bestaat_uit_buurten_identificatie"] == "03630023754008"
    assert records[0]["bestaat_uit_buurten_volgnummer"] == 1
    assert records[0]["begin_geldigheid"] == dtparse("2006-06-12T00:00:00.000000").date()
    assert records[0]["eind_geldigheid"] is None

    available_columns = {
        "id",
        "ggwgebieden_id",
        "bestaat_uit_buurten_id",
        "ggwgebieden_identificatie",
        "ggwgebieden_volgnummer",
        "bestaat_uit_buurten_identificatie",
        "bestaat_uit_buurten_volgnummer",
        "begin_geldigheid",
        "eind_geldigheid",
    }

    assert set(records[0].keys()) == available_columns


def test_event_process_nm_relation_update(here, tconn, local_metadata, gebieden_schema):
    """Prove that NM relations of an event get updated."""
    events_path = (
        here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten_update.gobevents"
    )
    importer = EventsProcessor(
        [gebieden_schema], tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [
        dict(r)
        for r in tconn.execute(
            """ SELECT * FROM gebieden_ggwgebieden_bestaat_uit_buurten
                ORDER BY bestaat_uit_buurten_identificatie """
        )
    ]
    assert len(records) == 2
    assert records[1]["bestaat_uit_buurten_id"] == "03630023754010.2"
    assert records[1]["begin_geldigheid"] == dtparse("2007-08-12T00:00:00.000000").date()


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
