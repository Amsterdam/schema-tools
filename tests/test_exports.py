from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import orjson
import pytest
import sqlalchemy_utils
from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from schematools.cli import export as export_cli
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
                folder, filename = blob_name.split("/", 1)
                if folder not in self.uploaded_blobs:
                    self.uploaded_blobs[folder] = {}
                self.uploaded_blobs[folder][filename] = {"data": data, "metadata": metadata}

        return DummyStorageClient()

    @pytest.fixture
    def meetbouten_content(self, here, connection, meetbouten_export_schema):
        ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
        importer = NDJSONImporter(meetbouten_export_schema, connection.engine)
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
        meetbouten_export_schema,
        export_schema_loader,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        importer = NDJSONImporter(gebieden_export_schema, connection.engine)
        importer.generate_db_objects("bouwblokken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("wijken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("stadsdelen", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("ggw_gebieden", truncate=False, ind_extra_index=False)
        importer = NDJSONImporter(meetbouten_export_schema, connection.engine)
        importer.generate_db_objects("meetbouten", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("metingen", truncate=False, ind_extra_index=False)
        export(connection, storage_client, loader=export_schema_loader, cleanup=False)
        local_files = [
            "gebieden_v1_bouwblokken_openbaar.csv",
            "gebieden_v1_buurten_openbaar.csv",
            "gebieden_v1_wijken_openbaar.csv",
            "gebieden_v1_stadsdelen_openbaar.csv",
            "gebieden_v1_ggw_gebieden_openbaar.csv",
            "gebieden_v1_bouwblokken_openbaar.geojson",
            "gebieden_v1_buurten_openbaar.geojson",
            "gebieden_v1_wijken_openbaar.geojson",
            "gebieden_v1_stadsdelen_openbaar.geojson",
            "gebieden_v1_ggw_gebieden_openbaar.geojson",
            "gebieden_v1_grote_gebieden_openbaar.gpkg",
            "gebieden_v1_stadsdelen_openbaar.gpkg",
            "gebieden_v1_ggw_gebieden_openbaar.gpkg",
            "gebieden_v1_wijken_openbaar.gpkg",
            "gebieden_v1_grote_gebieden_fp_mdw.gpkg",
            "gebieden_v1_stadsdelen_fp_mdw.gpkg",
            "gebieden_v1_ggw_gebieden_fp_mdw.gpkg",
            "gebieden_v1_wijken_fp_mdw.gpkg",
            "gebieden_v1_bouwblokken_fp_mdw.csv",
            "gebieden_v1_buurten_fp_mdw.csv",
            "gebieden_v1_bouwblokken_fp_mdw.jsonl",
            "gebieden_v1_buurten_fp_mdw.jsonl",
            "meet_bouten_v1_meetbouten_openbaar.csv",
            "meet_bouten_v1_all_openbaar.gpkg",
            "meet_bouten_v1_meetbouten_openbaar.gpkg",
            "meet_bouten_v1_meetbouten_openbaar.geojson",
            "meet_bouten_v1_meetbouten_openbaar.jsonl",
        ]
        storage_files = {
            "csv": [
                "gebieden_v1_kleine_gebieden_fp_mdw.csv.zip",
                "gebieden_v1_alle_gebieden_openbaar.csv.zip",
                "meet_bouten_v1_all_openbaar.csv.zip",
            ],
            "geojson": [
                "gebieden_v1_alle_gebieden_openbaar.geojson.zip",
                "meet_bouten_v1_all_openbaar.geojson.zip",
            ],
            "geopackage": [
                "gebieden_v1_grote_gebieden_openbaar.gpkg.zip",
                "gebieden_v1_grote_gebieden_fp_mdw.gpkg.zip",
                "meet_bouten_v1_all_openbaar.gpkg.zip",
            ],
            "jsonlines": [
                "gebieden_v1_kleine_gebieden_fp_mdw.jsonl.zip",
                "meet_bouten_v1_all_openbaar.jsonl.zip",
            ],
        }

        for zip_file in (
            storage_files["csv"]
            + storage_files["geojson"]
            + storage_files["geopackage"]
            + storage_files["jsonlines"]
        ):
            assert f"Created zip file {zip_file}." in caplog.text
            assert f"Uploaded {zip_file} to storage container" in caplog.text
            assert f"Removed local file {zip_file}." in caplog.text
        for file in local_files:
            assert caplog.text.count(f"Exporting {file}.") == 1, file
            path = Path("tmp", file)
            if not file.endswith("jsonl"):
                assert path.exists() and path.stat().st_size > 0
            path.unlink()
        Path("tmp").rmdir()
        for key in storage_files:
            assert list(storage_client.uploaded_blobs[key].keys()) == storage_files[key]
        for folder in storage_client.uploaded_blobs.values():
            for upload in folder.values():
                assert "table_ids" in upload["metadata"]
                assert len(upload["data"]) > 0

    def test_csv_export(self, meetbouten_export_schema, meetbouten_content, create_context):
        """Prove that csv export contains the correct content."""
        export_definition = meetbouten_export_schema.versions["v1"].exports[0]
        context = create_context(meetbouten_export_schema, export_definition)
        CsvExporter(context).export_tables()
        with open(context.folder / "meet_bouten_v1_meetbouten_openbaar.csv") as out_file:
            assert out_file.read() == (
                "Identificatie,Ligtinbuurtid,Merkcode,Merkomschrijving,Geometrie,Genesteinfonaam,Genesteinfonummer\n"
                "1,10180001.1,12,De meetbout,SRID=28992;POINT(119434 487091.6),,\n"
            )

    def test_csv_export_only_actual(self, gebieden_export_schema, create_context, connection):
        """Prove that csv export contains only the actual records, not the history."""
        importer = NDJSONImporter(gebieden_export_schema, connection.engine, logger=logger)
        importer.generate_db_objects("ggw_gebieden", truncate=True, ind_extra_index=False)
        connection.execute(
            text(
                "INSERT INTO gebieden_ggw_gebieden_v1 (id, identificatie, volgnummer, "
                "begin_geldigheid, eind_geldigheid) VALUES (1, 'ggw_gebied1', 1, '2020-01-01', "
                "'2020-12-31')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO gebieden_ggw_gebieden_v1 (id, identificatie, volgnummer, "
                "begin_geldigheid, eind_geldigheid) VALUES (2, 'ggw_gebied1', 2, '2021-01-01', NULL)"
            )
        )
        export_definition = next(
            exp
            for exp in gebieden_export_schema.versions["v1"].exports
            if exp.filetype == "csv" and "ggw_gebieden" in exp.table_ids
        )
        context = create_context(gebieden_export_schema, export_definition)
        CsvExporter(context).export_tables()
        with open(context.folder / "gebieden_v1_ggw_gebieden_openbaar.csv") as out_file:
            lines = out_file.readlines()
            assert len(lines) == 2  # includes the headerline
            assert lines[1].split(",")[0] == "2"  # volgnummer == 2

    def test_jsonlines_export(self, meetbouten_export_schema, meetbouten_content, create_context):
        """Prove that jsonlines export contains the correct content."""
        export_definition = next(
            exp
            for exp in meetbouten_export_schema.versions["v1"].exports
            if exp.filetype == "jsonl"
        )
        context = create_context(meetbouten_export_schema, export_definition)
        JsonLinesExporter(context).export_tables()
        with open(context.folder / "meet_bouten_v1_meetbouten_openbaar.jsonl") as out_file:
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

    def test_geopackage_export(self, meetbouten_export_schema, meetbouten_content, create_context):
        """Prove that geopackage export contains the correct content."""
        export_definition = next(
            exp
            for exp in meetbouten_export_schema.versions["v1"].exports
            if exp.filetype == "gpkg"
        )
        context = create_context(meetbouten_export_schema, export_definition)
        GeopackageExporter(context).export_tables()
        sqlite3_conn = sqlite3.connect(context.folder / "meet_bouten_v1_all_openbaar.gpkg")
        cursor = sqlite3_conn.cursor()
        cursor.execute("select * from rtree_meetbouten_v1_geometrie")
        res = cursor.fetchall()
        assert res == [(1, 119434.0, 119434.0, 487091.59375, 487091.65625)]
        cursor.execute(
            """
                select identificatie, ligt_in_buurt_id, merk_code, merk_omschrijving from meetbouten_v1
            """
        )
        res = cursor.fetchall()
        assert res == [(1, "10180001.1", "12", "De meetbout")]

    def test_geopackage_export_multiple_layers(self, gebieden_export_schema, create_context):
        """Prove that geopackage export contains the correct content."""
        export_definition = next(
            exp for exp in gebieden_export_schema.versions["v1"].exports if exp.filetype == "gpkg"
        )
        context = create_context(gebieden_export_schema, export_definition)

        # Ensure the DB tables exist so ogr2ogr can create layers.
        importer = NDJSONImporter(gebieden_export_schema, context.connection.engine)
        for table_id in ("stadsdelen", "ggw_gebieden", "wijken"):
            importer.generate_db_objects(table_id, truncate=False, ind_extra_index=False)

        GeopackageExporter(context).export_tables()
        sqlite3_conn = sqlite3.connect(context.folder / "gebieden_v1_grote_gebieden_openbaar.gpkg")
        cursor = sqlite3_conn.cursor()

        cursor.execute("SELECT table_name FROM gpkg_contents ORDER BY table_name;")
        layer_names = [row[0] for row in cursor.fetchall()]
        assert layer_names == [
            "ggw_gebieden_v1",
            "stadsdelen_v1",
            "wijken_v1",
        ]

        sqlite3_conn.close()

    def test_geojson_export(self, meetbouten_export_schema, meetbouten_content, create_context):
        """Prove that geojson export contains the correct content."""
        export_definition = next(
            exp
            for exp in meetbouten_export_schema.versions["v1"].exports
            if exp.filetype == "geojson"
        )
        context = create_context(meetbouten_export_schema, export_definition)
        GeoJsonExporter(context).export_tables()
        with open(context.folder / "meet_bouten_v1_meetbouten_openbaar.geojson") as out_file:
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
                    "genesteInfoNaam": None,
                    "genesteInfoNummer": None,
                },
                "geometry": {"type": "Point", "coordinates": [4.86497, 52.37055]},
            }

    def test_export_cli(self, connection, meetbouten_content):
        """Test the export CLI command."""
        runner = CliRunner()
        result = runner.invoke(
            export_cli,
            [
                "--db-url",
                connection.engine.url.render_as_string(hide_password=False),
                "--schema-url",
                str(Path(__file__).parent / "files" / "exports"),
                "meet_bouten",
                "--table-ids",
                "meetbouten",
                "--filetype",
                "csv",
                "--scopes",
                "openbaar",
            ],
        )
        assert result.exit_code == 0
        path = Path("tmp/meet_bouten_v1_meetbouten_openbaar.csv")
        with path.open() as out_file:
            assert out_file.read() == (
                "Identificatie,Ligtinbuurtid,Merkcode,Merkomschrijving,Geometrie,Genesteinfonaam,Genesteinfonummer\n"
                "1,10180001.1,12,De meetbout,SRID=28992;POINT(119434 487091.6),,\n"
            )
        path.unlink()
        Path("tmp").rmdir()
