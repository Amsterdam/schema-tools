import ndjson
from shapely.geometry import shape
from .base import BaseImporter


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(self, file_name, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        with open(file_name) as fh:
            for row in ndjson.reader(fh):
                wkt = shape(row["geometry"]).wkt
                row["geometry"] = f"SRID={self.srid};{wkt}"
                yield row
