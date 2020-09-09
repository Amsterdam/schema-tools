import ndjson
from shapely.geometry import shape
from .base import BaseImporter, Row
from schematools.utils import to_snake_case

from . import get_table_name
from schematools import RELATION_INDICATOR, MAX_TABLE_LENGTH


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(self, file_name, dataset_table, db_table_name=None, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        main_geometry = dataset_table.main_geometry
        fields_provenances = kwargs.pop("fields_provenances", {})
        identifier = dataset_table.identifier
        has_compound_key = dataset_table.has_compound_key
        if db_table_name is None:
            db_table_name = get_table_name(dataset_table)

        # Set up info for the special-case fields
        relation_field_info = []
        nm_relation_field_info = []
        inactive_relation_info = []
        jsonpath_provenance_info = []
        geo_fields = []
        for field in dataset_table.fields:
            # XXX maybe this is too much of a dirty hack and it would be better
            # to have an external configuration that determines which fields should
            # be flattened to strings
            comment = field.get("$comment")
            if comment is not None and "*stringify*" in comment:
                inactive_relation_info.append(field.name)
            if field.relation is not None:
                relation_field_info.append((field.name, field))
            field_provenance = field.provenance
            if field_provenance is not None and field_provenance.startswith("$"):
                jsonpath_provenance_info.append(field.name)
            if field.is_geo:
                geo_fields.append(field.name)
            if field.nm_relation is not None:
                _, related_table_name = [
                    to_snake_case(part) for part in field.nm_relation.split(":")
                ]
                nm_relation_field_info.append((field.name, related_table_name, field))

        with open(file_name) as fh:
            for _row in ndjson.reader(fh):
                row = Row(_row, fields_provenances=fields_provenances)
                for field_name in inactive_relation_info:
                    row[field_name] = str(row[field_name])
                for field_name in jsonpath_provenance_info:
                    row[field_name] = row[field_name]  # uses Row to get from object
                through_rows = {}
                for field_name in geo_fields:
                    geo_value = row[field_name]
                    if geo_value is not None:
                        wkt = shape(geo_value).wkt
                        row[field_name] = f"SRID={self.srid};{wkt}"
                # if main_geometry in row:
                #     main_geometry_value = row[main_geometry]
                #     if main_geometry_value is not None:
                #         wkt = shape(main_geometry_value).wkt
                #         row[main_geometry] = f"SRID={self.srid};{wkt}"
                id_value = ".".join(str(row[fn]) for fn in identifier)
                if has_compound_key:
                    row["id"] = id_value
                for relation_field_name, field in relation_field_info:
                    relation_field_value = row[relation_field_name]
                    if field.is_object:
                        fk_value_parts = []
                        for sub_field in field.sub_fields:
                            full_sub_field_name = sub_field.name
                            sub_field_name = full_sub_field_name.split(
                                RELATION_INDICATOR
                            )[1]
                            if relation_field_value is None:
                                sub_field_value = None
                            else:
                                sub_field_value = relation_field_value[sub_field_name]
                                fk_value_parts.append(sub_field_value)
                            row[full_sub_field_name] = sub_field_value
                        # empty fk_value_parts leads to None value
                        relation_field_value = ".".join(
                            (str(p) for p in fk_value_parts) or None
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
                            from_fk = id_value
                            through_row_record = {
                                f"{dataset_table.id}_id": from_fk,
                            }
                            # check is_through_table, add rows if needed
                            to_fk = value
                            if field.is_through_table:
                                through_field_names = field["items"][
                                    "properties"
                                ].keys()
                                to_fk = ".".join(
                                    str(value[fn]) for fn in through_field_names
                                )
                                for through_field_name in through_field_names:
                                    through_row_record[through_field_name] = value[
                                        through_field_name
                                    ]
                            through_row_record[f"{related_table_name}_id"] = to_fk
                            through_row_records.append(through_row_record)

                        field_name = to_snake_case(field.name)
                        through_table_id = f"{db_table_name}_{field_name}"[
                            :MAX_TABLE_LENGTH
                        ]
                        through_rows[through_table_id] = through_row_records

                    del row[nm_relation_field_name]
                yield {db_table_name: [row], **through_rows}
