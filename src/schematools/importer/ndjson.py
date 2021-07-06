import json

import ndjson
from shapely.geometry import shape

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR
from schematools.utils import to_snake_case

from . import get_table_name
from .base import BaseImporter, Row


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(self, file_name, dataset_table, db_table_name=None, **kwargs):
        """Provide an iterator the reads the NDJSON records"""
        fields_provenances = kwargs.pop("fields_provenances", {})
        identifier = dataset_table.identifier
        has_compound_key = dataset_table.has_compound_key
        if db_table_name is None:
            db_table_name = get_table_name(dataset_table)

        # Set up info for the special-case fields
        relation_field_info = []
        nm_relation_field_info = []
        nested_field_info = []
        inactive_relation_info = []
        jsonpath_provenance_info = []
        geo_fields = []
        for field in dataset_table.fields:
            # XXX maybe this is too much of a dirty hack and it would be better
            # to have an external configuration that determines which fields should
            # be flattened to strings
            comment = field.get("$comment")
            if comment is not None and "*stringify*" in comment:
                inactive_relation_info.append(field)
            if field.relation is not None:
                relation_field_info.append(field)
            field_provenance = field.provenance
            if field_provenance is not None and field_provenance.startswith("$"):
                jsonpath_provenance_info.append(field.name)
            if field.is_geo:
                geo_fields.append(field.name)
            if field.is_through_table and field.nm_relation is not None:
                nm_relation_field_info.append(field)
            if field.is_nested_table:
                nested_field_info.append(field)

        with open(file_name) as fh:
            for _row in ndjson.reader(fh):
                row = Row(_row, fields_provenances=fields_provenances)
                for ir_field in inactive_relation_info:
                    row[ir_field.name] = json.dumps(row[ir_field.id])
                for field_name in jsonpath_provenance_info:
                    row[field_name] = row[field_name]  # uses Row to get from object
                sub_rows = {}
                for field_name in geo_fields:
                    geo_value = row[field_name]
                    if geo_value is not None:
                        wkt = shape(geo_value).wkt
                        row[field_name] = f"SRID={self.srid};{wkt}"
                id_value = ".".join(str(row[fn]) for fn in identifier)
                if has_compound_key:
                    row["id"] = id_value

                for rel_field in relation_field_info:
                    relation_field_name = rel_field.name
                    relation_field_value = row[rel_field.id]
                    if rel_field.is_object:
                        fk_value_parts = []
                        for sub_field in rel_field.sub_fields:
                            # Ignore temporal fields
                            if sub_field.is_temporal:
                                continue
                            sub_field_id = sub_field.id.split(RELATION_INDICATOR)[1]
                            if relation_field_value is None:
                                sub_field_value = None
                            else:
                                sub_field_value = relation_field_value[sub_field_id]
                                fk_value_parts.append(sub_field_value)
                            row[sub_field.name] = sub_field_value
                        # empty fk_value_parts should result in None value
                        relation_field_value = ".".join([str(p) for p in fk_value_parts]) or None
                    row[f"{relation_field_name}_id"] = relation_field_value
                    del row[rel_field.id]

                for n_field in nested_field_info:
                    field_name = to_snake_case(n_field.name)
                    nested_row_records = []

                    if not row[n_field.id]:
                        continue
                    for nested_row in row[n_field.id]:
                        # When the identifier is compound, we can assume
                        # that an extra 'id' field will be available, because
                        # Django cannot live without it.
                        id_field = dataset_table.identifier
                        id_field_name = "id" if len(id_field) > 1 else id_field[0]
                        nested_row_record = {}
                        nested_row_record["parent_id"] = row[id_field_name]
                        for sub_field in n_field.sub_fields:
                            if sub_field.is_temporal:
                                continue
                            sub_field_name = to_snake_case(sub_field.name)
                            nested_row_record[sub_field_name] = nested_row.get(sub_field.name)

                        nested_row_records.append(nested_row_record)

                    sub_table_id = f"{db_table_name}_{field_name}"[:MAX_TABLE_NAME_LENGTH]
                    sub_rows[sub_table_id] = nested_row_records

                for nm_field in nm_relation_field_info:
                    values = row[nm_field.id]
                    if values is not None:
                        if not isinstance(values, list):
                            values = [values]

                        field_name = to_snake_case(nm_field.name)
                        through_row_records = []
                        for value in values:
                            from_fk = id_value
                            through_row_record = {
                                f"{dataset_table.name}_id": from_fk,
                            }

                            if dataset_table.has_compound_key:
                                for id_field in dataset_table.get_fields_by_id(
                                    *dataset_table.identifier
                                ):
                                    through_row_record[
                                        f"{dataset_table.name}_{to_snake_case(id_field.name)}"
                                    ] = row[id_field.name]
                            # check is_through_table, add rows if needed
                            to_fk = value
                            if nm_field.is_through_table:
                                through_field_metas = [
                                    (f.id.split(RELATION_INDICATOR)[-1], f.is_temporal)
                                    for f in nm_field.sub_fields
                                ]
                                to_fk = ".".join(
                                    str(value[fn])
                                    for fn, is_temporal in through_field_metas
                                    if not is_temporal
                                )
                                for through_field_name, is_temporal in through_field_metas:
                                    through_field_prefix = (
                                        "" if is_temporal else f"{nm_field.name}_"
                                    )
                                    full_through_field_name = to_snake_case(
                                        f"{through_field_prefix}{through_field_name}"
                                    )
                                    through_row_record[full_through_field_name] = value[
                                        through_field_name
                                    ]
                            through_row_record[f"{field_name}_id"] = to_fk

                            through_row_records.append(through_row_record)

                        sub_table_id = f"{db_table_name}_{field_name}"[:MAX_TABLE_NAME_LENGTH]
                        sub_rows[sub_table_id] = through_row_records

                    del row[nm_field.id]
                yield {db_table_name: [row], **sub_rows}
