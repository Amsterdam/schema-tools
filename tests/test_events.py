from datetime import datetime, date
from dateutil.parser import parse as dtparse
import pytest
from schematools.events import EventsProcessor

# pytestmark = pytest.mark.skip("all tests disabled")


def test_event_process_insert(here, tconn, local_metadata, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 2
    assert records[0]["code"] == "AA01"
    assert records[1]["code"] == "AA02"
    assert records[0]["eind_geldigheid"] is None
    assert records[0]["begin_geldigheid"] == date(2006, 6, 12)


def test_event_process_update(here, tconn, local_metadata, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken_update.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"
    assert records[0]["begin_geldigheid"] == date(2020, 2, 5)
    assert records[0]["registratiedatum"] == datetime(2020, 2, 5, 15, 6, 43)


def test_event_process_delete(here, tconn, local_metadata, gebieden_schema):
    events_path = here / "files" / "data" / "bouwblokken_delete.gobevents"
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 1
    assert records[0]["code"] == "AA01"


@pytest.mark.parametrize("use_dimension_fields", (True, False))
def test_event_process_1n_relation_insert(
    here, tconn, local_metadata, gebieden_schema, use_dimension_fields
):
    events_bb_path = here / "files" / "data" / "bouwblokken.gobevents"
    events_rel_path = (
        here / "files" / "data" / "gebieden_bouwblokken_ligt_in_buurt.gobevents"
    )
    gebieden_schema.use_dimension_fields = use_dimension_fields
    importer = EventsProcessor(
        [gebieden_schema],
        28992,
        tconn,
        local_metadata=local_metadata,
        truncate=True,
    )
    importer.load_events_from_file(events_bb_path)
    importer.load_events_from_file(events_rel_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 2
    assert records[1]["ligt_in_buurt_id"] == "03630000000707.2"
    assert records[1]["ligt_in_buurt_source_id"] == "03630012094861.1.AMSBI.geometrie"
    # Proper subset
    available_columns = {"ligt_in_buurt_source_id"}
    if use_dimension_fields:
        available_columns |= {
            "ligt_in_buurt_begin_geldigheid",
            "ligt_in_buurt_eind_geldigheid",
        }
    available_columns < records[0].keys()


@pytest.mark.parametrize("use_dimension_fields", (False, True))
def test_event_process_1n_relation_delete(
    here, tconn, local_metadata, gebieden_schema, use_dimension_fields
):
    events_bb_path = here / "files" / "data" / "bouwblokken.gobevents"
    events_rel_path = (
        here / "files" / "data" / "gebieden_bouwblokken_ligt_in_buurt_delete.gobevents"
    )
    gebieden_schema.use_dimension_fields = use_dimension_fields
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_bb_path)
    importer.load_events_from_file(events_rel_path)
    records = [dict(r) for r in tconn.execute("SELECT * from gebieden_bouwblokken")]
    assert len(records) == 2
    # The source_id need to be saved, for future updates
    assert records[1]["ligt_in_buurt_source_id"] is not None
    if use_dimension_fields:
        assert records[1]["ligt_in_buurt_begin_geldigheid"] is None
        assert records[1]["ligt_in_buurt_eind_geldigheid"] is None
    assert records[1]["ligt_in_buurt_identificatie"] is None
    assert records[1]["ligt_in_buurt_volgnummer"] is None
    assert records[1]["ligt_in_buurt_id"] is None


@pytest.mark.parametrize("use_dimension_fields", (True, False))
def test_event_process_nm_relation_insert(
    here,
    tconn,
    local_metadata,
    gebieden_schema,
    use_dimension_fields,
):
    events_rel_path = (
        here / "files" / "data" / "gebieden_ggwgebieden_bestaat_uit_buurten.gobevents"
    )
    gebieden_schema.use_dimension_fields = use_dimension_fields
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_rel_path)
    records = {
        r["source_id"]: dict(r)
        for r in tconn.execute("SELECT * from gebieden_ggwgebieden_bestaat_uit_buurten")
    }
    assert len(records) == 3
    assert (
        records["03630950000016.1.AMSBI.N65d"]["bestaat_uit_buurten_id"]
        == "03630000000658.1"
    )
    available_columns = {
        "source_id",
        "ggwgebieden_id",
        "bestaat_uit_buurten_id",
        "ggwgebieden_identificatie",
        "ggwgebieden_volgnummer",
        "bestaat_uit_buurten_identificatie",
        "bestaat_uit_buurten_volgnummer",
    }

    if use_dimension_fields:
        available_columns |= {"begin_geldigheid", "eind_geldigheid"}
    assert available_columns <= list(records.values())[0].keys()


@pytest.mark.parametrize("use_dimension_fields", (True, False))
def test_event_process_nm_relation_update(
    here, tconn, local_metadata, gebieden_schema, use_dimension_fields
):
    events_rel_path = (
        here
        / "files"
        / "data"
        / "gebieden_ggwgebieden_bestaat_uit_buurten_update.gobevents"
    )
    gebieden_schema.use_dimension_fields = use_dimension_fields
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_rel_path)
    records = {
        r["source_id"]: dict(r)
        for r in tconn.execute("SELECT * from gebieden_ggwgebieden_bestaat_uit_buurten")
    }
    assert len(records) == 3
    if use_dimension_fields:
        assert records["03630950000015.1.AMSBI.M50e"]["begin_geldigheid"] == dtparse(
            "2019-10-03T00:00:00.000000"
        )
    assert (
        records["03630950000015.1.AMSBI.M50e"]["bestaat_uit_buurten_id"]
        == "03630023754008.1"
    )


def test_event_process_nm_relation_delete(here, tconn, local_metadata, gebieden_schema):
    events_rel_path = (
        here
        / "files"
        / "data"
        / "gebieden_ggwgebieden_bestaat_uit_buurten_delete.gobevents"
    )
    importer = EventsProcessor(
        [gebieden_schema], 28992, tconn, local_metadata=local_metadata, truncate=True
    )
    importer.load_events_from_file(events_rel_path)
    records = {
        r["source_id"]: dict(r)
        for r in tconn.execute("SELECT * from gebieden_ggwgebieden_bestaat_uit_buurten")
    }
    assert len(records) == 2
    assert "03630950000015.1.AMSBI.M50e" not in records
