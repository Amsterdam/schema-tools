from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import orjson
import pytest
import sqlalchemy_utils
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from schematools.exports import export, logger
from schematools.exports.csv import CsvExporter
from schematools.exports.geojson import GeoJsonExporter
from schematools.exports.geopackage import GeopackageExporter
from schematools.exports.jsonlines import JsonLinesExporter
from schematools.importer.ndjson import NDJSONImporter
from schematools.types import ExportContext


class TestExports:
    @pytest.fixture
    def connection(self, db_url, sqlalchemy_keep_db):
        """Use a separate db for these tests, as they can interfere with the tests using the main
        test database."""
        engine = create_engine(
            db_url + "_exports",
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            connect_args={"cursor_factory": None},
        )
        db_exists = sqlalchemy_utils.functions.database_exists(engine.url)
        if db_exists and not sqlalchemy_keep_db:
            raise RuntimeError("DB exists, remove it before proceeding")

        if not db_exists:
            sqlalchemy_utils.functions.create_database(engine.url)
            with engine.connect() as connection:
                connection.execute(text("CREATE EXTENSION postgis"))
                connection.commit()
                yield connection
        sqlalchemy_utils.functions.drop_database(engine.url)
        engine.dispose()

    @pytest.fixture
    def storage_client(self):
        """Dummy in-memory storage with the same interface as Azure Blob Storage."""

        class DummyBlobClient:
            name: str

            def __init__(self, name, storage_client):
                self.name = name
                self.storage_client = storage_client

            def upload_blob(self, data, overwrite, metadata):
                self.storage_client.upload_blob(self.name, data.read(), metadata)

        class DummyContainerClient:
            def __init__(self, name, storage_client):
                self.name = name
                self.storage_client = storage_client

            def get_blob_client(self, blob_name):
                return DummyBlobClient(blob_name, self.storage_client)

        class DummyStorageClient:
            def __init__(self):
                self.uploaded_blobs: dict[str, Any] = {}

            def get_container_client(self, container_name):
                return DummyContainerClient(container_name, self)

            def upload_blob(self, blob_name, data, metadata):
                self.uploaded_blobs[blob_name] = {"data": data, "metadata": metadata}

        return DummyStorageClient()

    @pytest.fixture
    def meetbouten_content(self, here, connection, meetbouten_schema):
        ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
        importer = NDJSONImporter(meetbouten_schema, connection.engine)
        importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)

    @pytest.fixture
    def tmp_folder(self):
        """Creates a temporary folder for exports and cleans it up after the test."""
        path = Path("tmp")
        path.mkdir(exist_ok=True)
        yield path
        for file in path.iterdir():
            file.unlink()
        path.rmdir()

    @pytest.fixture
    def create_context(self, connection, storage_client, tmp_folder):
        """Factory fixture to create ExportContext objects for testing."""

        def create(dataset, export):
            return ExportContext(
                connection=connection,
                client=storage_client,
                dataset=dataset,
                export=export,
                folder=tmp_folder,
                size=1,
            )

        return create

    def test_export(
        self,
        connection,
        storage_client,
        gebieden_export_schema,
        export_schema_loader,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        importer = NDJSONImporter(gebieden_export_schema, connection.engine)
        importer.generate_db_objects("bouwblokken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("wijken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("stadsdelen", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("ggwgebieden", truncate=False, ind_extra_index=False)
        export(connection, storage_client, loader=export_schema_loader, cleanup=False)
        local_files = [
            "gebieden_v1_bouwblokken_openbaar.csv",
            "gebieden_v1_buurten_openbaar.csv",
            "gebieden_v1_wijken_openbaar.csv",
            "gebieden_v1_stadsdelen_openbaar.csv",
            "gebieden_v1_ggwgebieden_openbaar.csv",
            "gebieden_v1_bouwblokken_openbaar.geojson",
            "gebieden_v1_buurten_openbaar.geojson",
            "gebieden_v1_wijken_openbaar.geojson",
            "gebieden_v1_stadsdelen_openbaar.geojson",
            "gebieden_v1_ggwgebieden_openbaar.geojson",
            "gebieden_v1_stadsdelen_openbaar.gpkg",
            "gebieden_v1_ggwgebieden_openbaar.gpkg",
            "gebieden_v1_wijken_openbaar.gpkg",
            "gebieden_v1_stadsdelen_fp_mdw.gpkg",
            "gebieden_v1_ggwgebieden_fp_mdw.gpkg",
            "gebieden_v1_wijken_fp_mdw.gpkg",
            "gebieden_v1_bouwblokken_fp_mdw.csv",
            "gebieden_v1_buurten_fp_mdw.csv",
            "gebieden_v1_bouwblokken_fp_mdw.jsonl",
            "gebieden_v1_buurten_fp_mdw.jsonl",
        ]
        zip_files = [
            "gebieden_v1_alle_gebieden_openbaar.csv.zip",
            "gebieden_v1_alle_gebieden_openbaar.geojson.zip",
            "gebieden_v1_grote_gebieden_openbaar.gpkg.zip",
            "gebieden_v1_grote_gebieden_fp_mdw.gpkg.zip",
            "gebieden_v1_kleine_gebieden_fp_mdw.csv.zip",
            "gebieden_v1_kleine_gebieden_fp_mdw.jsonl.zip",
        ]
        for zip_file in zip_files:
            assert f"Created zip file {zip_file}." in caplog.text
            assert f"Uploaded {zip_file} to storage container" in caplog.text
            assert f"Removed local file {zip_file}." in caplog.text
        for file in local_files:
            assert caplog.text.count(f"Exporting {file}.") == 1
            path = Path("tmp", file)
            if not file.endswith("jsonl"):
                assert path.exists() and path.stat().st_size > 0
            path.unlink()
            # assert f"Removed local file {file}." in caplog.text
        Path("tmp").rmdir()
        assert list(storage_client.uploaded_blobs.keys()) == zip_files
        for upload in storage_client.uploaded_blobs.values():
            assert "table_ids" in upload["metadata"]
            assert len(upload["data"]) > 0

    def test_csv_export(self, meetbouten_schema, meetbouten_content, create_context):
        """Prove that csv export contains the correct content."""
        export_definition = meetbouten_schema.versions["v1"].exports[0]
        context = create_context(meetbouten_schema, export_definition)
        CsvExporter(context).export_tables()
        with open(context.folder / "meetbouten_v1_meetbouten_openbaar.csv") as out_file:
            assert out_file.read() == (
                "Identificatie,Ligtinbuurtid,Merkcode,Merkomschrijving,Geometrie\n"
                "1,10180001.1,12,De meetbout,SRID=28992;POINT(119434 487091.6)\n"
            )

    def test_csv_export_only_actual(self, gebieden_export_schema, create_context, connection):
        """Prove that csv export contains only the actual records, not the history."""
        importer = NDJSONImporter(gebieden_export_schema, connection.engine, logger=logger)
        importer.generate_db_objects("ggwgebieden", truncate=True, ind_extra_index=False)
        connection.execute(
            text(
                "INSERT INTO gebieden_ggwgebieden_v1 (id, identificatie, volgnummer, "
                "begin_geldigheid, eind_geldigheid) VALUES (1, 'ggwgebied1', 1, '2020-01-01', "
                "'2020-12-31')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO gebieden_ggwgebieden_v1 (id, identificatie, volgnummer, "
                "begin_geldigheid, eind_geldigheid) VALUES (2, 'ggwgebied1', 2, '2021-01-01', NULL)"
            )
        )
        export_definition = next(
            exp
            for exp in gebieden_export_schema.versions["v1"].exports
            if exp.filetype == "csv" and "ggwgebieden" in exp.table_ids
        )
        context = create_context(gebieden_export_schema, export_definition)
        CsvExporter(context).export_tables()
        with open(context.folder / "gebieden_v1_ggwgebieden_openbaar.csv") as out_file:
            lines = out_file.readlines()
            assert len(lines) == 2  # includes the headerline
            assert lines[1].split(",")[0] == "2"  # volgnummer == 2

    def test_jsonlines_export(self, meetbouten_schema, meetbouten_content, create_context):
        """Prove that jsonlines export contains the correct content."""
        export_definition = next(
            exp for exp in meetbouten_schema.versions["v1"].exports if exp.filetype == "jsonl"
        )
        context = create_context(meetbouten_schema, export_definition)
        JsonLinesExporter(context).export_tables()
        with open(context.folder / "meetbouten_v1_meetbouten_openbaar.jsonl") as out_file:
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

    def test_geopackage_export(self, meetbouten_schema, meetbouten_content, create_context):
        """Prove that geopackage export contains the correct content."""
        export_definition = next(
            exp for exp in meetbouten_schema.versions["v1"].exports if exp.filetype == "gpkg"
        )
        context = create_context(meetbouten_schema, export_definition)
        GeopackageExporter(context).export_tables()
        sqlite3_conn = sqlite3.connect(context.folder / "meetbouten_v1_meetbouten_openbaar.gpkg")
        cursor = sqlite3_conn.cursor()
        cursor.execute("select * from rtree_sql_statement_geometrie")
        res = cursor.fetchall()
        assert res == [(1, 119434.0, 119434.0, 487091.59375, 487091.65625)]
        cursor.execute(
            """
                select identificatie, ligt_in_buurt_id, merk_code, merk_omschrijving from sql_statement
            """
        )
        res = cursor.fetchall()
        assert res == [(1, "10180001.1", "12", "De meetbout")]

    def test_geojson_export(self, meetbouten_schema, meetbouten_content, create_context):
        """Prove that geojson export contains the correct content."""
        export_definition = next(
            exp for exp in meetbouten_schema.versions["v1"].exports if exp.filetype == "geojson"
        )
        context = create_context(meetbouten_schema, export_definition)
        GeoJsonExporter(context).export_tables()
        with open(context.folder / "meetbouten_v1_meetbouten_openbaar.geojson") as out_file:
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
