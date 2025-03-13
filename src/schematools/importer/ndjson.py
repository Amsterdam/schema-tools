from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import PosixPath
from typing import Any

import orjson
from shapely.geometry import shape

from schematools.types import DatasetFieldSchema, DatasetTableSchema

from .base import BaseImporter, Provenance, Record


class NDJSONImporter(BaseImporter):
    """Import an NDJSON file into the database."""

    def parse_records(
        self, file_name: PosixPath, dataset_table: DatasetTableSchema, **kwargs: Any
    ) -> Iterator[dict[str, list[Record]]]:
        """Provide an iterator the reads the NDJSON records."""
        # Initializes the field mapper once for the table
        field_mapper = TableFieldMapper(dataset_table)
        with open(file_name, "rb") as fh:
            for row in fh:
                if row != b"\n":
                    records = field_mapper.parse_object(orjson.loads(row))
                    yield records


class TableFieldMapper:
    """Conversion of field names during the import.
    The :meth:`parse_object` returns the database records to create for a given NDJson line.
    """

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
        "neuronId": "Id",
    }

    def __init__(self, dataset_table: DatasetTableSchema):
        """Analysis of fields that need special attention during the import."""
        self.dataset_table = dataset_table

        # Create cached provenance objects for all fields that have it.
        # The fields don't return a Provenance object themselves,
        # so provenance can also point to completely different datasource in the future.
        self.cached_provenance: dict[str, Provenance] = {
            prov: Provenance(prov) for field in dataset_table.fields if (prov := field.provenance)
        }

        # XXX maybe this is too much of a dirty hack, and it would be better
        # to have an external configuration that determines which fields should
        # be flattened to strings
        self.inactive_relation_info = {
            field.id
            for field in dataset_table.fields
            if (comment := field.get("$comment")) and "*stringify*" in comment
        }

        # When the table is a through-table, the GOB model fields need to be fetched elsewhere.
        self.through_field_map = {}
        if dataset_table.is_through_table:
            for prefix, through_field in zip(("src", "dst"), dataset_table.through_fields):
                self.through_field_map[through_field.id] = [
                    # generate "srcId", "dstId" and "srcVolgnummer", "dstVolgnummer" fields.
                    (related_identifier, f"{prefix}{self.id_name_mapping[related_identifier]}")
                    for related_identifier in through_field.related_table.identifier
                ]

    def parse_object(self, source: dict) -> dict[str, list[Record]]:
        """Parse the record, convert field names.
        Returns all database records to create, grouped by SQL table name.
        """
        main_row = Record(data={}, source=source)
        sub_rows = {}

        self._fix_through_fields(source)
        composite_key_filled = self._fill_composite_pk(source, main_row)

        # Fill all standard fields from the source.
        for field in self.dataset_table.get_fields(include_subfields=True):
            if field.id == "schema" and field.type.startswith("https://"):
                continue
            if field.id == "id" and composite_key_filled:
                # The composite key is already inserted in main_row
                # skip processing "id" field again
                continue

            # Read the value from the record. As subfields are also part of the main loop,
            # extra care is needed to make sure the correct dictionary is checked for the value.
            try:
                field_source = source[field.parent_field.id] if field.is_subfield else source
                value = self._get_value(field, field_source)
            except LookupError:
                if field.is_identifier_part:
                    # Avoid overriding the identifier we've just generated with None.
                    continue
                # Some missing fields still need to be mentioned in the insert statement.
                value = None

            if field.is_nested_table:
                # Nested object
                if value:
                    sub_rows[field.nested_table.id] = self._format_nested_rows(
                        field, value, main_row
                    )
            elif field.nm_relation is not None:
                # M2M through table
                if value:
                    sub_rows[field.through_table.id] = self._format_through_rows(
                        field, value, source
                    )
            elif field.relation is not None and field.is_object and isinstance(value, dict):
                # Foreign key to temporal (composite) key
                self._fill_composite_fk(field, value, main_row)
            else:
                # Any other field.
                main_row[field.db_name] = self._format_db_value(field, value)

        return {
            self.dataset_table.id: [main_row],
            **sub_rows,
        }

    def _fill_composite_pk(self, source: dict, row: Record) -> bool:
        """Adds a composite key 'id' for temporal tables.
        Return value indicates whether this was done.
        """
        temporal = not self.dataset_table.is_autoincrement and self.dataset_table.has_composite_key
        if not temporal:
            return False

        row["id"] = self._get_composite_id(source)
        return True

    def _get_value(self, field: DatasetFieldSchema, source: dict) -> Any:
        """Retrieve a single value from the source object.
        Raises LookupError when the value does not exist in the source.
        Note that any subfields are not treated differently,
        its assumed the 'source' is already at the correct level.
        """
        if source is None and field.is_subfield:
            raise LookupError(field.id)

        if field.provenance:
            # JSONPath or alias.
            return self.cached_provenance[field.provenance].resolve(source)
        else:
            # camelCase name
            return source[field.id]

    def _format_db_value(self, field: DatasetFieldSchema, value) -> Any:
        """Adjust the value for database format."""
        if value is None:
            return None
        elif field.is_geo:
            # Format geometry fields
            wkt = shape(value).wkt
            return f"SRID={field.srid};{wkt}"
        elif field.id in self.inactive_relation_info:
            # Convert nested object to JSON string
            return json.dumps(value)
        elif field.is_json_object:
            return value
        elif isinstance(value, (dict, list)):
            raise ValueError(
                f"Value of '{field.qualified_id}' should resolve to a scalar, not: {value!r}"
            )
        else:
            return value

    def _get_composite_id(self, row: dict) -> str:
        """Concat identifier fields for a single composite field value"""
        return ".".join(str(row[fn]) for fn in self.dataset_table.identifier)

    def _fix_through_fields(self, source: dict):
        """Maps fields in ndjson to proper fieldnames and structure.

        When through tables (1-N and NM relations) are imported directly
        from the GOB GraphQL API, the fieldnames in the ndjson are not
        following the names of the associated Amsterdam Schema.

        The importer is built around the Amsterdam Schema fieldnames.
        So, incoming ndjson records needs to be mapped to the appropriate
        amsterdam schema field names, to be able to import the ndjson
        records correctly.
        """
        for through_field_id, gob_names in self.through_field_map.items():
            if len(gob_names) > 1:  # composite key
                source[through_field_id] = {
                    identifier: source[gob_name] for identifier, gob_name in gob_names
                }
            else:
                gob_name = gob_names[0][1]
                source[through_field_id] = source[gob_name]

    def _fill_composite_fk(self, rel_field: DatasetFieldSchema, value: dict | None, row: Record):
        """Process a composite foreign key"""
        # Flatten subfields, as they are part of the same record.
        fk_value_parts = []
        relation_attributes = rel_field.relation_attributes
        for subfield in rel_field.subfields:
            # Any date-ranges of a composite key are not inlined in the main object
            if subfield.is_temporal_range or subfield.id in relation_attributes:
                continue

            if value is None:
                subfield_value = None
            else:
                subfield_value = value[subfield.id]
                fk_value_parts.append(subfield_value)

            row[subfield.db_name] = subfield_value

        # Make sure composite keys also get their Django _id field.
        # Empty fk_value_parts should result in None value
        row[rel_field.db_name] = ".".join([str(p) for p in fk_value_parts]) or None

    def _format_nested_rows(
        self, n_field: DatasetFieldSchema, value: list[dict], main_row: Record
    ) -> list[Record]:
        """Collect the records for a nested field"""
        # When the identifier is composite, we can assume
        # that an extra 'id' field will be available, because
        # Django cannot live without it.
        parent_id_fields = self.dataset_table.identifier
        parent_id_field = "id" if len(parent_id_fields) > 1 else parent_id_fields[0]
        parent_id = main_row[parent_id_field]

        return [
            Record(
                {
                    "parent_id": parent_id,
                    **self._get_field_values(n_field.subfields, nested_row),
                },
                source=nested_row,
            )
            for nested_row in value
        ]

    def _get_field_values(self, fields: Iterator[DatasetFieldSchema], nested_row: dict):
        """Extract any (nested) values from a row."""
        row = {}
        for field in fields:
            try:
                row[field.db_name] = self._get_value(field, nested_row)
            except LookupError:
                pass  # skip unmentioned fields

        return row

    def _format_through_rows(
        self, nm_field: DatasetFieldSchema, values: list[dict], source: dict
    ) -> list[Record]:
        """Provide the records for a Many-to-Many (NM) fields."""
        source_db_name = self.dataset_table.db_name_variant(
            with_dataset_prefix=False, with_version=False
        )
        src_id_value = self._get_composite_id(source)
        if not isinstance(values, list):
            values = [values]

        through_row_records = []
        for value in values:  # type: dict[str, Any]
            # Generate foreignkey value for right hand side
            dst_id_value = ".".join(
                str(value[field.id]) for field in nm_field.subfields if not field.is_temporal_range
            )

            # Generate the main id values that Django needs
            through_row_record = Record(
                {
                    f"{source_db_name}_id": src_id_value,
                    f"{nm_field.db_name}_id": dst_id_value,
                },
                source=value,
            )

            # Fill the temporal identifier fields if these are present.
            for prefix, table, source_dict in (
                (source_db_name, self.dataset_table, source),
                (nm_field.db_name, nm_field.related_table, value),
            ):
                if table.has_composite_key:
                    for id_field in table.identifier_fields:
                        subfield_id = f"{prefix}_{id_field.db_name}"
                        through_row_record[subfield_id] = source_dict[id_field.id]

            # Fill any temporal dimensions if these are part of the through record
            for subfield in nm_field.subfields:
                if subfield.is_temporal_range:
                    try:
                        through_row_record[subfield.db_name] = self._get_value(subfield, value)
                    except KeyError:
                        pass

            through_row_records.append(through_row_record)

        return through_row_records
