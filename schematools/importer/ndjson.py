import ndjson
from shapely.geometry import shape
from .base import BaseImporter

from . import get_table_name


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(self, file_name, dataset_table, db_table_name=None, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        main_geometry = dataset_table.main_geometry
        identifier = dataset_table.identifier
        if db_table_name is None:
            db_table_name = get_table_name(dataset_table)
        relation_field_names = [
            field.name for field in dataset_table.fields if field.relation is not None
        ]
        nm_relation_field_names = [
            (field.name, field.nm_relation.split(":")[1])
            for field in dataset_table.fields
            if field.nm_relation is not None
        ]
        with open(file_name) as fh:
            for row in ndjson.reader(fh):
                through_rows = {}
                if main_geometry in row:
                    wkt = shape(row[main_geometry]).wkt
                    row[main_geometry] = f"SRID={self.srid};{wkt}"
                for relation_field_name in relation_field_names:
                    row[f"{relation_field_name}_id"] = row[relation_field_name]
                    del row[relation_field_name]
                for (
                    nm_relation_field_name,
                    related_table_name,
                ) in nm_relation_field_names:
                    values = row[nm_relation_field_name]
                    if values is not None:
                        if not isinstance(values, list):
                            values = [values]

                        through_rows[f"{db_table_name}_{nm_relation_field_name}"] = [
                            {
                                f"{dataset_table.id}_id": row[identifier],
                                f"{related_table_name}_id": value,
                            }
                            for value in values
                        ]
                    del row[nm_relation_field_name]
                yield {db_table_name: [row], **through_rows}
