from __future__ import annotations

import gc
import logging
import os
import re
import zipfile
from pathlib import Path

from sqlalchemy import Connection

from schematools.exports.base import BaseExporter
from schematools.exports.csv import CsvExporter
from schematools.exports.geojson import GeoJsonExporter
from schematools.exports.geopackage import GeopackageExporter
from schematools.exports.jsonlines import JsonLinesExporter
from schematools.loaders import CachedSchemaLoader, get_schema_loader
from schematools.types import ExportContext, ExportFileType, StorageClient

logger = logging.getLogger(__name__)

FILETYPE_TO_EXPORTER: dict[ExportFileType, type[BaseExporter]] = {
    "csv": CsvExporter,
    "gpkg": GeopackageExporter,
    "jsonl": JsonLinesExporter,
    "geojson": GeoJsonExporter,
}


def sanitize(input_string):
    # Remove any characters not supported by 'latin-1' encoding
    return re.sub(r"[^\x00-\x7F]+", "", input_string)


def export_tables(context: ExportContext):
    exporter_class = FILETYPE_TO_EXPORTER[context.export.filetype]
    exporter = exporter_class(context)
    exporter.export_tables()


def zip_files(context: ExportContext) -> Path:
    output_path = context.folder / context.export.filename
    Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "a", compression=zipfile.ZIP_DEFLATED) as zipf:
        for file_path in context.export.table_paths(context.folder):
            zipf.write(file_path, file_path.name)
    logger.info("Created zip file %s.", output_path.name)
    return output_path


def upload_to_storage(path: Path, context: ExportContext, metadata: dict[str, str]):
    container_name = "bulk-data" if context.export.is_public else "bulk-data-fp-mdw"
    container_client = context.client.get_container_client(container_name)
    with path.open("rb") as zf:
        blob_client = container_client.get_blob_client(context.export.filename)
        blob_client.upload_blob(
            zf,
            overwrite=True,
            metadata={**metadata, "table_ids": context.export.table_ids},
        )
        logger.info(
            "Uploaded %s to storage container %s.", context.export.filename, container_name
        )
    path.unlink()  # remove the zip file after uploading
    logger.info("Removed local file %s.", context.export.filename)
    gc.collect()


def remove_files(file_paths: list[Path]):
    for file_path in file_paths:
        if file_path.exists():
            file_path.unlink()
            logger.info("Removed local file %s.", file_path.name)
    gc.collect()


def export(
    connection: Connection,
    storage_client: StorageClient,
    output_path: str = "tmp",
    *,
    loader: CachedSchemaLoader | None = None,  # For testing purposes.
    cleanup: bool = True,  # For testing purposes.
):
    """Exports all defined exports from the database to the configured storage."""
    loader = loader or get_schema_loader()
    path = Path(output_path)
    path.mkdir(parents=True, exist_ok=True)
    for dataset_name, dataset in loader.get_all_datasets().items():
        logger.info("Exporting dataset %s.", dataset_name)
        dataset_metadata: dict[str, str] = {
            k: sanitize(v) for k, v in dataset.data.items() if isinstance(v, str)
        }
        file_paths = []
        for version in dataset.versions.values():
            for export in version.exports:
                context = ExportContext(
                    connection=connection,
                    dataset=dataset,
                    folder=path,
                    export=export,
                    client=storage_client,
                )
                logger.info("Exporting %s", export)
                export_tables(context)
                zip_path = zip_files(context)
                upload_to_storage(zip_path, context, dataset_metadata)
                file_paths.extend(context.export.table_paths(context.folder))

        if cleanup:
            remove_files(file_paths)
            gc.collect()
