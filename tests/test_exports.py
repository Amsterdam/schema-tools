from __future__ import annotations

import logging
import shlex
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
from schematools.types import Export, ExportContext


class TestExports:
    @pytest.fixture
    def engine(self, db_url, sqlalchemy_keep_db):
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
            yield engine
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
    def meetbouten_content(self, here, engine, meetbouten_export_schema):
        ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
        importer = NDJSONImporter(meetbouten_export_schema, engine)
        importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
        importer.load_file(ndjson_path)

    @pytest.fixture
    def fietspaaltjes_content(self, here, engine, fietspaaltjes_export_schema):
        ndjson_path = here / "files" / "data" / "fietspaaltjes.ndjson"
        importer = NDJSONImporter(fietspaaltjes_export_schema, engine)
        importer.generate_db_objects("fietspaaltjes", truncate=True, ind_extra_index=False)
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
    def create_context(self, engine, storage_client, tmp_folder):
        """Factory fixture to create ExportContext objects for testing."""

        def create(dataset, export):
            return ExportContext(
                engine=engine,
                client=storage_client,
                dataset=dataset,
                export=export,
                folder=tmp_folder,
                size=1,
            )

        return create

    def test_export(
        self,
        engine,
        storage_client,
        gebieden_export_schema,
        meetbouten_export_schema,
        fietspaaltjes_export_schema,
        export_schema_loader,
        caplog,
    ):
        caplog.set_level(logging.INFO)
        importer = NDJSONImporter(gebieden_export_schema, engine)
        importer.generate_db_objects("bouwblokken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("wijken", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("stadsdelen", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("ggw_gebieden", truncate=False, ind_extra_index=False)
        importer = NDJSONImporter(meetbouten_export_schema, engine)
        importer.generate_db_objects("meetbouten", truncate=False, ind_extra_index=False)
        importer.generate_db_objects("metingen", truncate=False, ind_extra_index=False)
        importer = NDJSONImporter(fietspaaltjes_export_schema, engine)
        importer.generate_db_objects("fietspaaltjes", truncate=False, ind_extra_index=False)
        export(engine, storage_client, loader=export_schema_loader, cleanup=False)
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
            "fietspaaltjes_v1_fietspaaltjes_openbaar.csv",
        ]
        storage_files = {
            "csv": [
                "fietspaaltjes_v1_all_openbaar.csv.zip",
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

    def test_export_failure_skips_publish_and_returns_failures(
        self,
        engine,
        storage_client,
        export_schema_loader,
        fietspaaltjes_export_schema,
        fietspaaltjes_content,
        tmp_folder,
        monkeypatch,
        caplog,
    ):
        caplog.set_level(logging.INFO)

        # Limit the batch export to a single dataset/export to keep the test focused.
        monkeypatch.setattr(
            export_schema_loader,
            "get_all_datasets",
            lambda: {"fietspaaltjes": fietspaaltjes_export_schema},
        )

        def _boom(*_args, **_kwargs):
            raise RuntimeError("forced exporter failure")

        monkeypatch.setattr(CsvExporter, "write_rows", _boom)

        failures = export(
            engine,
            storage_client,
            output_path=str(tmp_folder),
            loader=export_schema_loader,
            cleanup=True,
        )

        assert failures, "Expected a non-empty failure report"
        failure = failures[0]
        assert failure.filename == "fietspaaltjes_v1_all_openbaar.csv"
        assert failure.table_id == "fietspaaltjes"
        assert failure.error_type == "RuntimeError"
        assert failure.error_message == "forced exporter failure"
        assert str(failure) == (
            "Failed to export table 'fietspaaltjes' for export 'fietspaaltjes_v1_all_openbaar.csv':"
            " RuntimeError - forced exporter failure"
        )

        # Publishing must be skipped when any table export fails.
        assert storage_client.uploaded_blobs == {}
        assert "Created zip file fietspaaltjes_v1_all_openbaar.csv.zip." not in caplog.text
        assert "Uploaded fietspaaltjes_v1_all_openbaar.csv.zip" not in caplog.text

        # When cleanup=True, artifacts for failed exports should be removed.
        assert not (tmp_folder / "fietspaaltjes_v1_fietspaaltjes_openbaar.csv").exists()

    def test_export_continues_after_export_failure(
        self,
        engine,
        storage_client,
        export_schema_loader,
        meetbouten_export_schema,
        meetbouten_content,
        tmp_folder,
        monkeypatch,
        caplog,
    ):
        """If a single export fails, the batch run should continue with the next export."""
        caplog.set_level(logging.INFO)

        # Limit the batch export to a single dataset to keep the test focused.
        monkeypatch.setattr(
            export_schema_loader,
            "get_all_datasets",
            lambda: {"meet_bouten": meetbouten_export_schema},
        )

        version = meetbouten_export_schema.versions[meetbouten_export_schema.default_version]
        csv_export = next(exp for exp in version.exports if exp.filetype == "csv")
        jsonl_export = next(exp for exp in version.exports if exp.filetype == "jsonl")

        # Ensure deterministic ordering: first export fails, second should still succeed.
        monkeypatch.setattr(version, "exports", [csv_export, jsonl_export])

        # Ensure all tables referenced by these exports exist in the DB.
        importer = NDJSONImporter(meetbouten_export_schema, engine)
        for table_id in sorted(set(csv_export.table_ids + jsonl_export.table_ids)):
            if table_id == "meetbouten":
                continue  # already created + populated by meetbouten_content
            importer.generate_db_objects(table_id, truncate=False, ind_extra_index=False)

        def _boom(*_args, **_kwargs):
            raise RuntimeError("forced exporter failure")

        monkeypatch.setattr(CsvExporter, "write_rows", _boom)

        failures = export(
            engine,
            storage_client,
            output_path=str(tmp_folder),
            loader=export_schema_loader,
            cleanup=True,
        )

        # First export (CSV) failed and was reported.
        assert any(
            f.filename == csv_export.filename_without_zip
            and f.error_type == "RuntimeError"
            and f.error_message == "forced exporter failure"
            for f in failures
        )

        # Second export (JSONL) still got published.
        assert "Uploaded" in caplog.text
        assert f"Uploaded {jsonl_export.filename} to storage container" in caplog.text
        assert f"Uploaded {csv_export.filename} to storage container" not in caplog.text

        assert "csv" not in storage_client.uploaded_blobs
        assert list(storage_client.uploaded_blobs["jsonlines"].keys()) == [jsonl_export.filename]

    def test_base_exporter_retries_and_writes_atomically(
        self,
        gebieden_export_schema,
        create_context,
        monkeypatch,
    ):
        export_definition = next(
            exp
            for exp in gebieden_export_schema.versions["v1"].exports
            if exp.name == "kleine_gebieden" and exp.filetype == "csv"
        )
        context = create_context(gebieden_export_schema, export_definition)

        first_table_id = export_definition.table_ids[0]
        call_counts: dict[str, int] = {}

        def _flaky_write_rows(self, file_handle, table, *_args, **_kwargs):
            call_counts[table.id] = call_counts.get(table.id, 0) + 1
            if table.id == first_table_id and call_counts[table.id] < 3:
                file_handle.write("partial\n")
                raise RuntimeError("boom")
            file_handle.write("ok\n")

        monkeypatch.setattr(CsvExporter, "write_rows", _flaky_write_rows)

        failures = CsvExporter(context).export_tables(max_attempts=3, delay_seconds=0)
        assert failures == []

        out_path = context.folder / context.export.table_filename(first_table_id)
        assert out_path.exists() and out_path.stat().st_size > 0
        assert out_path.read_text(encoding="utf8") == "ok\n"
        assert call_counts[first_table_id] == 3

    def test_base_exporter_aborts_after_retries_exhausted(
        self,
        gebieden_export_schema,
        create_context,
        monkeypatch,
    ):
        export_definition = next(
            exp
            for exp in gebieden_export_schema.versions["v1"].exports
            if exp.name == "kleine_gebieden" and exp.filetype == "csv"
        )
        context = create_context(gebieden_export_schema, export_definition)

        first_table_id, second_table_id = export_definition.table_ids[:2]
        call_counts: dict[str, int] = {}

        def _always_fail_first_table(self, file_handle, table, *_args, **_kwargs):
            call_counts[table.id] = call_counts.get(table.id, 0) + 1
            if table.id == first_table_id:
                file_handle.write("partial\n")
                raise RuntimeError("boom")
            file_handle.write("ok\n")

        monkeypatch.setattr(CsvExporter, "write_rows", _always_fail_first_table)

        failures = CsvExporter(context).export_tables(max_attempts=3, delay_seconds=0)
        assert len(failures) == 1
        failure = failures[0]
        assert failure.filename == export_definition.filename_without_zip
        assert failure.table_id == first_table_id
        assert failure.error_type == "RuntimeError"
        assert failure.error_message == "boom"

        first_out = context.folder / context.export.table_filename(first_table_id)
        second_out = context.folder / context.export.table_filename(second_table_id)
        assert not first_out.exists()
        assert not second_out.exists()

        assert call_counts[first_table_id] == 3
        assert second_table_id not in call_counts

        assert list(context.folder.iterdir()) == []

    def test_csv_array_fields(
        self, fietspaaltjes_export_schema, fietspaaltjes_content, create_context
    ):
        """Prove that csv export handles array of strings fields correctly
        and array of object fields get filtered out."""
        export_definition = fietspaaltjes_export_schema.versions["v1"].exports[0]
        context = create_context(fietspaaltjes_export_schema, export_definition)

        CsvExporter(context).export_tables()
        with open(context.folder / "fietspaaltjes_v1_fietspaaltjes_openbaar.csv") as out_file:
            assert out_file.read() == (
                "Id,Geometry,Street,At,Area,Score2013,Scorecurrent,Count,Paaltjesweg,Soortpaaltje"
                ",Uiterlijk,Type,Ruimte,Markering,Beschadigingen,Veiligheid,Zichtindonker,Soortweg,Noodzaak\n"
                "Fietsplaatje record met display"
                ",SRID=28992;POINT(119434 487092.6)"
                ",Weesperplein,Geschutswerf,Amsterdam-Centrum,,reference for DISPLAY FIELD,6"
                ",nu paaltje(s)"  # paaltjes_weg: 1 item
                ',"paaltje(s) ong. 75cm hoog,verwijderde paaltjes"'  # soort_paaltje: array of 2
                ",rood/wit"  # uiterlijk
                ',"vast,uitneembaar"'  # type, array of 2
                ",Voldoende: 1.6m of meer"  # ruimte, 1 item
                ',"markering ontbreekt,onvoldoende markering"'  # markering: array of 2
                ",,overzichtelijke locatie,onvoldoende reflectie op paal"
                ',"rijbaan fiets+auto,fietspad"'  # soort_weg: array of 2
                ",nodig tegen sluipverkeer\n"  # noodzaak: 1 item
            )

    def test_csv_export_only_actual(self, gebieden_export_schema, create_context, engine):
        """Prove that csv export contains only the actual records, not the history."""
        importer = NDJSONImporter(gebieden_export_schema, engine, logger=logger)
        importer.generate_db_objects("ggw_gebieden", truncate=True, ind_extra_index=False)
        with engine.connect() as connection:
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
            connection.commit()
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
        importer = NDJSONImporter(gebieden_export_schema, context.engine)
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

    def test_export_cli(self, engine, meetbouten_content):
        """Test the export CLI command."""
        runner = CliRunner()
        result = runner.invoke(
            export_cli,
            [
                "--db-url",
                engine.url.render_as_string(hide_password=False),
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

    def test_geopackage_export_retries_then_succeeds(
        self,
        gebieden_export_schema,
        create_context,
        monkeypatch,
    ) -> None:
        export_definition = next(
            exp for exp in gebieden_export_schema.versions["v1"].exports if exp.filetype == "gpkg"
        )
        context = create_context(gebieden_export_schema, export_definition)

        calls: list[str] = []

        def fake_run(cmd, *_args, **_kwargs):
            calls.append(cmd)
            if len(calls) == 1:
                raise RuntimeError("boom")
            args = shlex.split(cmd)
            if "-nln" in args:  # noqa: SIM108
                output_path_index = 4 if "-update" in args else 3
            else:
                output_path_index = 3
            Path(args[output_path_index]).touch()
            return None

        monkeypatch.setattr("schematools.exports.geopackage.subprocess.run", fake_run)

        failures = GeopackageExporter(context).export_tables(max_attempts=2, delay_seconds=0)
        assert failures == []
        assert any("-nln" in call for call in calls)
        assert len(calls) == (len(export_definition.tables) * 2) + 1

    def test_geopackage_export_skips_merge_for_single_table(
        self,
        meetbouten_export_schema,
        create_context,
        monkeypatch,
    ) -> None:
        base_export_definition = next(
            exp
            for exp in meetbouten_export_schema.versions["v1"].exports
            if exp.filetype == "gpkg" and len(exp.tables) == 1
        )
        export_definition = Export(
            name="meetbouten",
            tables=base_export_definition.tables,
            scopes=base_export_definition.scopes,
            filetype=base_export_definition.filetype,
            version=base_export_definition.version,
            _dataset_name=meetbouten_export_schema.id,
        )
        context = create_context(meetbouten_export_schema, export_definition)

        calls: list[str] = []

        def fake_run(cmd, *, shell, check, **_kwargs):
            calls.append(cmd)
            args = shlex.split(cmd)
            Path(args[3]).touch()
            return None

        monkeypatch.setattr("schematools.exports.geopackage.subprocess.run", fake_run)

        failures = GeopackageExporter(context).export_tables(max_attempts=1, delay_seconds=0)

        assert failures == []
        assert len(calls) == 1
        assert "-nln" not in calls[0]
        assert (context.folder / export_definition.filename_without_zip).exists()

    def test_geopackage_export_retries_then_fails_permanently(
        self,
        meetbouten_export_schema,
        create_context,
        monkeypatch,
    ) -> None:
        base_export_definition = next(
            exp
            for exp in meetbouten_export_schema.versions["v1"].exports
            if exp.filetype == "gpkg" and len(exp.tables) == 1
        )
        export_definition = Export(
            name="retry_test",
            tables=base_export_definition.tables,
            scopes=base_export_definition.scopes,
            filetype=base_export_definition.filetype,
            version=base_export_definition.version,
            _dataset_name=meetbouten_export_schema.id,
        )
        context = create_context(meetbouten_export_schema, export_definition)

        calls: list[str] = []

        def fake_run(cmd, *, shell, check, **_kwargs):
            calls.append(cmd)
            raise RuntimeError("boom")

        monkeypatch.setattr("schematools.exports.geopackage.subprocess.run", fake_run)

        failures = GeopackageExporter(context).export_tables(max_attempts=3, delay_seconds=0)
        assert len(failures) == 1
        failure = failures[0]
        assert failure.filename == export_definition.filename_without_zip
        assert failure.table_id == export_definition.tables[0].id
        assert failure.error_type == "RuntimeError"
        assert failure.error_message == "boom"

        assert len(calls) == 3  # aborts before merge
        consolidated = context.folder / export_definition.filename_without_zip
        assert not consolidated.exists()
