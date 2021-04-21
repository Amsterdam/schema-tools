"""Exporter module."""
from __future__ import annotations

from collections import defaultdict

from geoalchemy2.shape import to_shape
from json_encoder import json
from sqlalchemy import Table
from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.events.factories import tables_factory
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import to_snake_case


def fetch_complex_fields_info(dataset_table: DatasetTableSchema) -> dict[str, dict]:
    """Collect info about complex fields (mainly relations)."""
    complex_fields = {}

    for field in dataset_table.fields:
        multi = None
        properties = {}
        relation_ds = None
        try:
            if (nm_relation := field.nm_relation) is not None:
                multi = True
                properties = field["items"]["properties"]
                relation_ds = nm_relation.split(":")[0]
            if (relation := field.relation) is not None:
                multi = False
                properties = field["properties"]
                relation_ds = relation.split(":")[0]
        except KeyError:
            continue

        if multi is not None:
            complex_fields[to_snake_case(field.name)] = {
                "multi": multi,
                "relation_ds": relation_ds,
                "identifier_names": dataset_table.identifier,
                "sub_field_names": [to_snake_case(sf) for sf in properties.keys()],
            }
    return complex_fields


def collect_nm_embed_rows(
    dataset_id, table_id, datasets_lookup, tables, complex_fields_info, connection: Connection
):
    """Fetch row info as list of embeddable objects."""
    nm_embeds = defaultdict(lambda: defaultdict(list))
    for field_name, field_info in complex_fields_info.items():
        if field_info["multi"]:
            through_table = tables[dataset_id][f"{table_id}_{field_name}"]
            for row in connection.execute(through_table.select()):
                row_dict = dict(row)
                id_value = ".".join(
                    str(row_dict[f"{table_id}_{idn}"]) for idn in field_info["identifier_names"]
                )

                stripped_row = {}
                for sfn in field_info["sub_field_names"]:
                    stripped_row[sfn] = row_dict[f"{field_name}_{sfn}"]
                nm_embeds[table_id][id_value].append(stripped_row)
    return nm_embeds


def fetch_nm_embeds(row, table_id, nm_embed_rows, complex_fields_info):
    """Fetch row info as lists of embeddables."""
    nm_embeds = defaultdict(list)
    for field_name, field_info in complex_fields_info.items():
        if field_info["multi"]:
            id_value = ".".join(str(row[idn]) for idn in field_info["identifier_names"])
            row_dicts = nm_embed_rows[table_id].get(id_value)
            nm_embeds[field_name] = row_dicts

    return nm_embeds


def fetch_1n_embeds(row, complex_fields_info):
    """Fetch row info as embeddable object(s)."""
    embeddable_objs = {}
    for field_name, field_info in complex_fields_info.items():
        if not field_info["multi"]:
            embed_obj = {}
            for sub_field_name in field_info["sub_field_names"]:
                embed_obj[sub_field_name] = row[f"{field_name}_{sub_field_name}"]
            embeddable_objs[field_name] = embed_obj
    return embeddable_objs


def export_events(datasets, dataset_id: str, table_id: str, connection: Connection):
    """Export the events from the indicated dataset and table."""
    tables: dict[str, dict[str, Table]] = {}
    datasets_lookup: dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
    dataset_table: DatasetTableSchema = datasets_lookup[dataset_id].get_table_by_id(table_id)
    geo_fields = [to_snake_case(field.name) for field in dataset_table.fields if field.is_geo]

    complex_fields_info = fetch_complex_fields_info(dataset_table)

    for ds_id, dataset in datasets_lookup.items():
        tables[ds_id] = tables_factory(dataset, metadata)

    # Collect in one go (to prevent multiple queries)
    nm_embed_rows = collect_nm_embed_rows(
        dataset_id, table_id, datasets_lookup, tables, complex_fields_info, connection
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
        row.update(fetch_1n_embeds(row, complex_fields_info))
        row.update(fetch_nm_embeds(row, table_id, nm_embed_rows, complex_fields_info))
        event_parts.append(json.dumps(row))
        yield "|".join(event_parts)
