"""Exporter module.

This module facilitates the export of events from GOB database tables.
The main objective of this module is to create full events that can be used
to test the event-importer that is also part of the `schematools.events` package.

The `schematools` cli interface had an entry to use the code in this module.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from geoalchemy2.shape import to_shape
from json_encoder import json
from sqlalchemy import Table
from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.events.factories import tables_factory
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import to_snake_case


@dataclass
class ComplexFieldAttrs:
    field_name: str
    is_many: Optional[bool] = None
    relation_ds: Optional[str] = None
    identifier_names: List[str] = field(default_factory=list)
    sub_field_names: List[str] = field(default_factory=list)


def fetch_complex_fields_metadata(dataset_table: DatasetTableSchema) -> List[ComplexFieldAttrs]:
    """Collect info about complex fields (mainly relations)."""
    complex_fields_metadata = []

    for complex_field in dataset_table.complex_fields:
        is_many = None
        properties = {}
        relation_ds = None
        try:
            if (nm_relation := complex_field.nm_relation) is not None:
                is_many = True
                properties = complex_field["items"]["properties"]
                relation_ds = nm_relation.split(":")[0]
            if (relation := complex_field.relation) is not None:
                is_many = False
                properties = complex_field["properties"]
                relation_ds = relation.split(":")[0]
        except KeyError:
            # Simple (non-object) relations do not have "properties" nor "items".
            # Those relations are not relevant for the export,
            # so they will be skipped.
            continue

        if is_many is not None:
            complex_fields_metadata.append(
                ComplexFieldAttrs(
                    complex_field_name=to_snake_case(complex_field.name),
                    is_many=is_many,
                    relation_ds=relation_ds,
                    identifier_names=dataset_table.identifier,
                    sub_field_names=[to_snake_case(sf) for sf in properties.keys()],
                )
            )

    return complex_fields_metadata


def collect_nm_embed_rows(
    dataset_id: str,
    table_id: str,
    datasets_lookup: Dict[str, DatasetSchema],
    tables: Dict[str, Dict[str, Table]],
    complex_fields_metadata: List[ComplexFieldAttrs],
    connection: Connection,
) -> Dict[str, Any]:
    """Fetch row info as list of embeddable objects."""
    nm_embeds = defaultdict(lambda: defaultdict(list))
    for field_metadata in complex_fields_metadata:
        if field_metadata.is_many:
            through_table = tables[dataset_id][f"{table_id}_{field_metadata.field_name}"]
            for row in connection.execute(through_table.select()):
                row_dict = dict(row)
                id_value = ".".join(
                    str(row_dict[f"{table_id}_{idn}"]) for idn in field_metadata.identifier_names
                )

                stripped_row = {}
                for sfn in field_metadata.sub_field_names:
                    stripped_row[sfn] = row_dict[f"{field_metadata.field_name}_{sfn}"]
                nm_embeds[table_id][id_value].append(stripped_row)
    return dict(nm_embeds)


def fetch_nm_embeds(
    row: Dict[str, Any],
    table_id: str,
    nm_embed_rows: Dict[str, Any],
    complex_fields_metadata: List[ComplexFieldAttrs],
) -> Dict[str, list[Dict[str, Any]]]:
    """Fetch row info as lists of embeddables."""
    nm_embeds = defaultdict(list)
    for field_metadata in complex_fields_metadata:
        if field_metadata.is_many:
            id_value = ".".join(str(row[idn]) for idn in field_metadata.identifier_names)
            row_dicts = nm_embed_rows[table_id].get(id_value)
            nm_embeds[field_metadata.field_name] = row_dicts

    return dict(nm_embeds)


def fetch_1n_embeds(
    row: Dict[str, Any], complex_fields_metadata: List[ComplexFieldAttrs]
) -> Dict[str, Dict[str, Any]]:
    """Fetch row info as embeddable object(s)."""
    embeddable_objs = {}
    for field_metadata in complex_fields_metadata:
        if not field_metadata.is_many:
            embed_obj = {}
            for sub_field_name in field_metadata.sub_field_names:
                embed_obj[sub_field_name] = row[f"{field_metadata.field_name}_{sub_field_name}"]
            embeddable_objs[field_metadata.field_name] = embed_obj
    return embeddable_objs


def export_events(
    datasets, dataset_id: str, table_id: str, connection: Connection
) -> Iterator[str]:
    """Export the events from the indicated dataset and table."""
    tables: Dict[str, Dict[str, Table]] = {}
    datasets_lookup: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
    dataset_table: DatasetTableSchema = datasets_lookup[dataset_id].get_table_by_id(table_id)
    geo_fields = [to_snake_case(field.name) for field in dataset_table.fields if field.is_geo]

    complex_fields_metadata: List[ComplexFieldAttrs] = fetch_complex_fields_metadata(dataset_table)

    for ds_id, dataset in datasets_lookup.items():
        tables[ds_id] = tables_factory(dataset, metadata)

    # Collect in one go (to prevent multiple queries)
    nm_embed_rows: Dict[str, Any] = collect_nm_embed_rows(
        dataset_id, table_id, datasets_lookup, tables, complex_fields_metadata, connection
    )
    for r in connection.execute(tables[dataset_id][table_id].select()):
        row = dict(r)
        meta = {"event_type": "ADD", "dataset_id": dataset_id, "table_id": table_id}
        id_ = ".".join(str(row[f]) for f in dataset_table.identifier)
        event_parts = [f"{dataset_id}.{table_id}.{id_}", json.dumps(meta)]
        for geo_field in geo_fields:
            geom = row.get(geo_field)
            if geom:
                row[geo_field] = f"SRID={geom.srid};{to_shape(geom).wkt}"
        row.update(fetch_1n_embeds(row, complex_fields_metadata))
        row.update(fetch_nm_embeds(row, table_id, nm_embed_rows, complex_fields_metadata))
        event_parts.append(json.dumps(row))
        yield "|".join(event_parts)
