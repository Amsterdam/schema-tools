from __future__ import annotations

import logging
import subprocess

from psycopg import sql

from schematools.exports.base import BaseExporter
from schematools.types import ExportTableFailure

logger = logging.getLogger(__name__)


class GeopackageExporter(BaseExporter):
    extension = "gpkg"

    def export_tables(self) -> list[ExportTableFailure]:
        failures: list[ExportTableFailure] = []
        pg_conn_str = (
            f"host={self.engine.url.host} "
            f"port={self.engine.url.port} "
            f"dbname={self.engine.url.database} "
            f"user={self.engine.url.username} "
            f"password={self.engine.url.password}"
        )
        consolidated_file = self.base_dir / self.export.filename_without_zip
        logger.info("Exporting %s.", self.export.filename_without_zip)

        exported_table_ids: set[str] = set()

        for table in self.tables:
            filename = self.export.table_filename(table.id)
            output_path = self.base_dir / filename
            if output_path.exists():
                logger.warning("File %s already exists. It will be skipped.", output_path.name)
                exported_table_ids.add(table.id)
                continue
            logger.info("Exporting %s.", filename)
            field_names = sql.SQL(",").join(
                sql.Identifier(field.db_name)
                for field in self._get_fields(table)
                if field.db_name != "schema"
            )
            if not next(field_names.__iter__(), None):
                continue

            table_name = sql.Identifier(table.db_name)
            query = sql.SQL("SELECT {field_names} from {table_name}").format(
                field_names=field_names, table_name=table_name
            )
            if self.size is not None:
                query = sql.SQL("{query} LIMIT {size}").format(
                    query=query, size=sql.Literal(self.size)
                )

            try:
                export_cmd = (
                    f'ogr2ogr -f "GPKG" {output_path} PG:"{pg_conn_str}" '
                    f'-sql "{query.as_string()}"'
                )
                subprocess.run(  # noqa: S602
                    export_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                exported_table_ids.add(table.id)
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    ExportTableFailure(
                        dataset_id=self.dataset_schema.id,
                        dataset_version=self.export.version,
                        export_name=self.export.name,
                        scopes=self.export.scopes_string,
                        filetype=self.export.filetype,
                        table_id=table.id,
                        output_path=str(output_path),
                        attempts=1,
                        error={"type": type(exc).__name__, "message": str(exc)},
                    )
                )

        merged_any = False
        for table in self.tables:
            if table.id not in exported_table_ids:
                continue
            filename = self.export.table_filename(table.id)
            output_path = self.base_dir / filename
            flag = "" if not merged_any else "-update"
            try:
                merge_cmd = (
                    f'ogr2ogr -f "GPKG" {flag} {consolidated_file} {output_path} '
                    f"-nln {table.db_name_variant(with_dataset_prefix=False)}"
                )
                subprocess.run(  # noqa: S602
                    merge_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                merged_any = True
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    ExportTableFailure(
                        dataset_id=self.dataset_schema.id,
                        dataset_version=self.export.version,
                        export_name=self.export.name,
                        scopes=self.export.scopes_string,
                        filetype=self.export.filetype,
                        table_id=table.id,
                        output_path=str(consolidated_file),
                        attempts=1,
                        error={"type": type(exc).__name__, "message": str(exc)},
                    )
                )

        return failures
