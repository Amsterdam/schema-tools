from __future__ import annotations

import logging
import subprocess
import time

from psycopg import sql

from schematools.exports.base import BaseExporter
from schematools.types import ExportTableFailure

logger = logging.getLogger(__name__)


class GeopackageExporter(BaseExporter):
    extension = "gpkg"

    def _build_query(self, table) -> sql.SQL | None:

        field_names = sql.SQL(",").join(
            sql.Identifier(field.db_name)
            for field in self._get_fields(table)
            if field.db_name != "schema"
        )
        if not next(field_names.__iter__(), None):
            return None

        table_name = sql.Identifier(table.db_name)

        # Join related table when mainGeometry is a relation
        if table.has_main_geometry and (rel_table := table.main_geometry_field.related_table):
            field_names = sql.SQL(",").join(
                sql.SQL("current_table.{field_name}").format(
                    field_name=sql.Identifier(field.db_name)
                )
                for field in self._get_fields(table)
                if field.db_name != "schema"
            )

            right_pk = rel_table.get_field_by_id(rel_table.identifier[0])

            query = sql.SQL(
                "SELECT {field_names}, related_table.{related_geometry} AS geometry "
                "FROM {table_name} AS current_table "
                "LEFT JOIN {related_table} AS related_table "
                "ON current_table.{left_fk} = related_table.{right_pk}"
            ).format(
                field_names=field_names,
                related_geometry=sql.Identifier(rel_table.main_geometry_field.db_name),
                table_name=sql.Identifier(table.db_name),
                related_table=sql.Identifier(rel_table.db_name),
                left_fk=sql.Identifier(table.main_geometry_field.db_name),
                right_pk=sql.Identifier(right_pk.db_name),
            )

        else:
            query = sql.SQL("SELECT {field_names} from {table_name}").format(
                field_names=field_names,
                table_name=table_name,
            )

        if self.size is not None:
            query = sql.SQL("{query} LIMIT {size}").format(
                query=query,
                size=sql.Literal(self.size),
            )

        return query

    def export_tables(
        self,
        *,
        max_attempts: int = 3,
        delay_seconds: int = 1,
    ) -> list[ExportTableFailure]:
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
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.warning("File %s already exists. It will be skipped.", output_path.name)
                exported_table_ids.add(table.id)
                continue
            if output_path.exists():
                # Remove zero-byte/partial artifacts from previous runs.
                output_path.unlink()
            logger.info("Exporting %s.", filename)

            query = self._build_query(table)
            if query is None:
                continue

            query_string = query.as_string()
            last_exc: Exception | None = None

            export_cmd = (
                f'ogr2ogr -f "GPKG" "{output_path}" PG:"{pg_conn_str}" -sql "{query_string}" '
                f"{table.db_name_variant(with_dataset_prefix=False)}"
            )

            for attempt in range(1, max_attempts + 1):
                try:
                    subprocess.run(  # noqa: S602
                        export_cmd,
                        shell=True,
                        check=True,
                    )
                    exported_table_ids.add(table.id)
                    last_exc = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.error(
                        "ogr2ogr export failed for %s (attempt %s/%s): %s",
                        table.id,
                        attempt,
                        max_attempts,
                        exc,
                    )

                    if output_path.exists():
                        output_path.unlink()
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)

            if last_exc is not None:
                return [
                    ExportTableFailure(
                        filename=self.export.filename_without_zip,
                        table_id=table.id,
                        error_type=type(last_exc).__name__,
                        error_message=str(last_exc),
                    )
                ]

        tables_to_merge = [table for table in self.tables if table.id in exported_table_ids]
        if not tables_to_merge:
            return []

        last_exc: Exception | None = None
        last_table_id = tables_to_merge[0].id

        for attempt in range(1, max_attempts + 1):
            try:
                merged_any = False
                for table in tables_to_merge:
                    last_table_id = table.id
                    input_path = self.base_dir / self.export.table_filename(table.id)
                    if input_path == consolidated_file:
                        continue

                    flag = "" if not merged_any else "-update"
                    merge_cmd = (
                        f'ogr2ogr -f "GPKG" {flag} {consolidated_file} {input_path} '
                        f"-nln {table.db_name_variant(with_dataset_prefix=False)}"
                    )
                    subprocess.run(  # noqa: S602
                        merge_cmd,
                        shell=True,
                        check=True,
                    )
                    merged_any = True
                last_exc = None
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.error(
                    "Merge failed for %s (attempt %s/%s): %s",
                    last_table_id,
                    attempt,
                    max_attempts,
                    exc,
                )

                if consolidated_file.exists():
                    consolidated_file.unlink()
                if attempt < max_attempts:
                    time.sleep(delay_seconds)

        if last_exc is not None:
            return [
                ExportTableFailure(
                    filename=self.export.filename_without_zip,
                    table_id=last_table_id,
                    error_type=type(last_exc).__name__,
                    error_message=str(last_exc),
                )
            ]

        return []
