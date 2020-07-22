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
        relation_field_info = [
            (field.name, field)
            for field in dataset_table.fields
            if field.relation is not None
        ]
        nm_relation_field_info = [
            (field.name, field.nm_relation.split(":")[1], field)
            for field in dataset_table.fields
            if field.nm_relation is not None
        ]
        with open(file_name) as fh:
            for row in ndjson.reader(fh):
                through_rows = {}
                if main_geometry in row:
                    wkt = shape(row[main_geometry]).wkt
                    row[main_geometry] = f"SRID={self.srid};{wkt}"
                for relation_field_name, field in relation_field_info:
                    relation_field_value = row[relation_field_name]
                    if field.is_object:
                        fk_value_parts = []
                        for sub_field in field.sub_fields:
                            full_sub_field_name = sub_field.name
                            sub_field_name = full_sub_field_name.split("__")[1]
                            sub_field_value = relation_field_value[sub_field_name]
                            row[sub_field_name] = sub_field_value
                            fk_value_parts.append(sub_field_value)
                        relation_field_value = ".".join(
                            (str(p) for p in fk_value_parts)
                        )
                    row[f"{relation_field_name}_id"] = relation_field_value
                    del row[relation_field_name]
                for (
                    nm_relation_field_name,
                    related_table_name,
                    field,
                ) in nm_relation_field_info:
                    values = row[nm_relation_field_name]
                    if values is not None:
                        if not isinstance(values, list):
                            values = [values]

                        through_row_records = []
                        for value in values:
                            through_row_record = {
                                f"{dataset_table.id}_id": row[identifier],
                            }
                            # check is_through_table, add rows if needed
                            scalar_value = value
                            if field.is_through_table:
                                scalar_value = value[dataset_table.identifier]
                                for through_field_name in field["items"][
                                    "properties"
                                ].keys():
                                    through_row_record[through_field_name] = value[
                                        through_field_name
                                    ]
                            through_row_record[
                                f"{related_table_name}_id"
                            ] = scalar_value
                            through_row_records.append(through_row_record)

                        through_rows[
                            f"{db_table_name}_{nm_relation_field_name}"
                        ] = through_row_records

                    del row[nm_relation_field_name]
                yield {db_table_name: [row], **through_rows}
