import os
from pathlib import Path
from tempfile import mkdtemp

from schematools.types import DatasetSchema


def export_geopackages(
    db_url: str,
    dataset_schema: DatasetSchema,
    table_ids: list[str] | None = None,
    base_dir_str: str | None = None,
) -> None:
    """Export geopackages for all tables or an indicated subset in the dataset."""
    base_dir = Path(base_dir_str or mkdtemp())
    tables = (
        dataset_schema.tables
        if not table_ids
        else [dataset_schema.get_table_by_id(table_id) for table_id in table_ids]
    )
    command = 'ogr2ogr -f "GPKG" {output_path} PG:"{db_url}" -sql "{sql}"'
    for table in tables:
        output_path = base_dir / f"{table.db_name}.gpkg"
        sql = f"SELECT * from {table.db_name}"  # noqa: S608  # nosec: B608
        os.system(  # noqa: S605  # nosec: B605
            command.format(output_path=output_path, db_url=db_url, sql=sql)
        )
