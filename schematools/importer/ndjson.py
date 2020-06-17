import ndjson
from shapely.geometry import shape
from .base import BaseImporter


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(self, file_name, dataset_table, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        main_geometry = dataset_table.main_geometry
        relation_field_names = [
            field.name for field in dataset_table.fields if field.relation is not None
        ]
        with open(file_name) as fh:
            for row in ndjson.reader(fh):
                if main_geometry is not None:
                    wkt = shape(row[main_geometry]).wkt
                    row[main_geometry] = f"SRID={self.srid};{wkt}"
                for relation_field_name in relation_field_names:
                    row[f"{relation_field_name}_id"] = row[relation_field_name]
                    del row[relation_field_name]
                yield row
