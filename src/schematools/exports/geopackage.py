from __future__ import annotations

import logging
import subprocess

from psycopg import sql

from schematools.exports.base import BaseExporter

logger = logging.getLogger(__name__)


class GeopackageExporter(BaseExporter):
    def export_tables(self):
        pg_conn_str = (
            f"host={self.engine.url.host} "
            f"port={self.engine.url.port} "
            f"dbname={self.engine.url.database} "
            f"user={self.engine.url.username} "
            f"password={self.engine.url.password}"
        )
        consolidated_file = self.base_dir / self.export.filename_without_zip
        logger.info("Exporting %s.", self.export.filename_without_zip)

        for table in self.tables:
            filename = self.export.table_filename(table.id)
            output_path = self.base_dir / filename
            if output_path.exists():
                logger.warning("File %s already exists. It will be skipped.", output_path.name)
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

            subprocess.run(  # noqa: S602
                f'ogr2ogr -f "GPKG" {output_path} PG:"{pg_conn_str}" -sql "{query.as_string()}"',
                shell=True,
            )
        for table, idx in zip(self.tables, range(len(self.tables)), strict=True):
            flag = "" if idx == 0 else "-update"
            filename = self.export.table_filename(table.id)
            output_path = self.base_dir / filename
            subprocess.run(  # noqa: S602
                f'ogr2ogr -f "GPKG" {flag} {consolidated_file} {output_path} '
                f"-nln {table.db_name_variant(with_dataset_prefix=False)}",
                shell=True,
            )
