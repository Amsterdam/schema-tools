import json
from pathlib import PosixPath
from typing import Any, Callable, Dict, Iterator, List, Optional, cast

import ndjson
from shapely.geometry import shape

from schematools import MAX_TABLE_NAME_LENGTH, RELATION_INDICATOR
from schematools.types import DatasetTableSchema
from schematools.utils import to_snake_case

from . import get_table_name
from .base import BaseImporter, Row


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def _get_through_fields_mapper(
        self, dataset_table: DatasetTableSchema
    ) -> Optional[Callable[[Row], Row]]:
        """Maps fields in ndjson to proper fieldnames and structure.

        When through tables (1-N and NM relations) are imported directly
        from the GOB graphql API, the fieldnames in the ndjson are not
        following the names of the associated amsterdam schema.

        The importer is built around the amsterdam schema fieldnames.
        So, incoming ndjson records needs to be mapped to the appropriate
        amsterdam schema field names, to be able to import the ndjson
        records correctly.

        """
        field_mapping = {}
        # This is mapping the GOBModel `entity_id` to `Id`
        # that needs to be used in GraphQL queries.
        id_name_mapping = {
            "identificatie": "Id",
            "volgnummer": "Volgnummer",
            "dossier": "Id",
            "documentnummer": "Id",
            "vestigingsnummer": "Id",
            "sbiActiviteitNummer": "Id",
            "kvknummer": "Id",
            "wozdeelobjectnummer": "Id",
            "wozobjectnummer": "Id",
            "id": "Id",
            "code": "Id",
        }
        through_field_ids = dataset_table.data.get("throughFields")
        if through_field_ids is None:
            return None

        for direction_prefix, field_id in zip(("src", "dst"), through_field_ids):
            field = dataset_table.get_field_by_id(field_id)
            if field is None:
                self.logger.log_warning("No through field found: %s", field_id)
                return None

            related_table = field.related_table
            if related_table is None:
                self.logger.log_warning("No related_table found for: %s", field_id)
                return None

            field_mapping[field_id] = [
                (idf, f"{direction_prefix}{id_name_mapping[idf]}")
                for idf in related_table.identifier
            ]

        def _map_fields(row: Row) -> Row:
            for field_id, idfs__in_names in field_mapping.items():
                if len(idfs__in_names) > 1:  # composite key
                    row[field_id] = {idf: row[in_name] for idf, in_name in idfs__in_names}
                else:
                    row[field_id] = row[idfs__in_names[0][1]]
            return row

        return _map_fields

    def parse_records(  # type: ignore[override]
        self,
        file_name: PosixPath,
        dataset_table: DatasetTableSchema,
        db_table_name: Optional[str] = None,
        is_through_table: bool = False,
        **kwargs: Any,
    ) -> Iterator[Dict[str, List[Row]]]:
        """Provide an iterator the reads the NDJSON records."""
        fields_provenances = kwargs.pop("fields_provenances", {})
        identifier = dataset_table.identifier
        has_composite_key = dataset_table.has_composite_key
        through_fields_mapper = None
        if is_through_table:
            through_fields_mapper = self._get_through_fields_mapper(dataset_table)

        if db_table_name is None:
            db_table_name = get_table_name(dataset_table)

        table_name = to_snake_case(dataset_table.name)

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
                if through_fields_mapper is not None:
                    _row = through_fields_mapper(_row)
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

                if not dataset_table.is_autoincrement:
                    id_value = ".".join(str(row[fn]) for fn in identifier)
                    if has_composite_key:
                        row["id"] = id_value

                for rel_field in relation_field_info:
                    relation_field_name = to_snake_case(rel_field.name)
                    # Only process relation if data is available in incoming row
                    if rel_field.id not in row:
                        continue
                    relation_field_value = row[rel_field.id]
                    if rel_field.is_object:
                        fk_value_parts = []
                        for subfield in rel_field.get_subfields(add_prefixes=True):
                            # Ignore temporal fields
                            if subfield.is_temporal:
                                continue

                            subfield_id = subfield.id.rsplit(RELATION_INDICATOR, 1)[1]
                            if relation_field_value is None:
                                subfield_value = None
                            else:
                                subfield_value = relation_field_value[subfield_id]
                                fk_value_parts.append(subfield_value)
                            row[to_snake_case(subfield.name)] = subfield_value
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
                        # When the identifier is composite, we can assume
                        # that an extra 'id' field will be available, because
                        # Django cannot live without it.
                        id_fields = dataset_table.identifier
                        id_field_name = "id" if len(id_fields) > 1 else id_fields[0]
                        nested_row_record = {}
                        nested_row_record["parent_id"] = row[id_field_name]
                        for subfield in n_field.subfields:
                            if subfield.is_temporal:
                                continue
                            subfield_name = to_snake_case(subfield.name)
                            nested_row_record[subfield_name] = nested_row.get(subfield.name)

                        nested_row_records.append(nested_row_record)

                    sub_table_id = f"{table_name}_{field_name}"[:MAX_TABLE_NAME_LENGTH]
                    sub_rows[sub_table_id] = nested_row_records

                for nm_field in nm_relation_field_info:

                    # Only process relation if data is available in incoming row
                    if nm_field.id not in row:
                        continue

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

                            if dataset_table.has_composite_key:
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
                                    for f in nm_field.subfields
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
                            # PK has been changed to an autonumber
                            # through_row_record["id"] = f"{from_fk}.{to_fk}"
                            through_row_record[f"{field_name}_id"] = to_fk

                            through_row_records.append(through_row_record)

                        sub_table_id = f"{table_name}_{field_name}"[:MAX_TABLE_NAME_LENGTH]
                        sub_rows[sub_table_id] = through_row_records

                    del row[nm_field.id]
                yield {table_name: [row], **{k: cast(List[Row], v) for k, v in sub_rows.items()}}
