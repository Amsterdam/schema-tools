from __future__ import annotations

import sqlite3

import orjson
import pytest
from sqlalchemy import text

from schematools.exports.csv import export_csvs
from schematools.exports.geojson import export_geojsons
from schematools.exports.geopackage import export_geopackages
from schematools.exports.jsonlines import export_jsonls
from schematools.importer.ndjson import NDJSONImporter


def _load_meetbouten_content(here, engine, meetbouten_schema):
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)


def test_csv_export(here, engine, meetbouten_schema, dbsession, tmp_path):
    """Prove that csv export contains the correct content."""
    _load_meetbouten_content(here, engine, meetbouten_schema)
    with engine.begin() as connection:
        export_csvs(connection, meetbouten_schema, str(tmp_path), [], [], 1)
        with open(tmp_path / "meetbouten_meetbouten.csv") as out_file:
            assert out_file.read() == (
                "Identificatie,Ligtinbuurtid,Merkcode,Merkomschrijving,Geometrie\n"
                "1,10180001.1,12,De meetbout,SRID=28992;POINT(119434 487091.6)\n"
            )


def test_csv_export_only_actual(here, engine, ggwgebieden_schema, dbsession, tmp_path):
    """Prove that csv export contains only the actual records, not the history."""
    ndjson_path = here / "files" / "data" / "ggwgebieden-history.ndjson"
    importer = NDJSONImporter(ggwgebieden_schema, engine)
    importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    with engine.begin() as connection:
        export_csvs(connection, ggwgebieden_schema, str(tmp_path), [], [], 1)
        with open(tmp_path / "ggwgebieden_ggwgebieden.csv") as out_file:
            lines = out_file.readlines()
            assert len(lines) == 2  # includes the headerline
            assert lines[1].split(",")[0] == "2"  # volgnummer == 2


def test_jsonlines_export(here, engine, meetbouten_schema, dbsession, tmp_path):
    """Prove that jsonlines export contains the correct content."""
    with engine.begin() as connection:
        _load_meetbouten_content(here, engine, meetbouten_schema)
        export_jsonls(connection, meetbouten_schema, str(tmp_path), [], [], 1)
        with open(tmp_path / "meetbouten_meetbouten.jsonl") as out_file:
            result = orjson.loads(out_file.read())
            result["geometrie"]["coordinates"][1] = round(result["geometrie"]["coordinates"][1], 5)
            result["geometrie"]["coordinates"][0] = round(result["geometrie"]["coordinates"][0], 5)
            assert result == {
                "identificatie": 1,
                "ligtInBuurtId": "10180001.1",
                "merkCode": "12",
                "merkOmschrijving": "De meetbout",
                "geometrie": {"type": "Point", "coordinates": [4.86497, 52.37055]},
            }


# We have to skip this test, ogr2og2 is not available on github
# We need to think of a flag to enable this test locally with `skipif`.
@pytest.mark.skip()
def test_geopackage_export(here, engine, meetbouten_schema, dbsession, tmp_path):
    """Prove that geopackage export contains the correct content."""
    _load_meetbouten_content(here, engine, meetbouten_schema)
    with engine.begin() as connection:
        export_geopackages(connection, meetbouten_schema, str(tmp_path), [], [])
    sqlite3_conn = sqlite3.connect(tmp_path / "meetbouten_meetbouten.gpkg")
    cursor = sqlite3_conn.cursor()
    cursor.execute(text("select * from rtree_sql_statement_geometrie"))
    res = cursor.fetchall()
    assert res == [(1, 119434.0, 119434.0, 487091.59375, 487091.65625)]
    cursor.execute(
        """
            select identificatie, ligt_in_buurt_id, merk_code, merk_omschrijving from sql_statement
       """
    )
    res = cursor.fetchall()
    assert res == [(1, "10180001.1", "12", "De meetbout")]


def test_geojson_export(here, engine, meetbouten_schema, dbsession, tmp_path):
    """Prove that geojson export contains the correct content."""
    with engine.begin() as connection:
        _load_meetbouten_content(here, engine, meetbouten_schema)
        export_geojsons(connection, meetbouten_schema, str(tmp_path), [], [], 1)
        with open(tmp_path / "meetbouten_meetbouten.geojson") as out_file:
            result = orjson.loads(out_file.read())
            feature = result["features"][0]
            feature["geometry"]["coordinates"][1] = round(feature["geometry"]["coordinates"][1], 5)
            feature["geometry"]["coordinates"][0] = round(feature["geometry"]["coordinates"][0], 5)
            assert feature == {
                "type": "Feature",
                "properties": {
                    "identificatie": 1,
                    "ligtInBuurtId": "10180001.1",
                    "merkCode": "12",
                    "merkOmschrijving": "De meetbout",
                },
                "geometry": {"type": "Point", "coordinates": [4.86497, 52.37055]},
            }
