__doc__ = """

Direct de tabel bijwerken, of via een tussenstap (met validaties)
Validaties wschl. eerder in het proces (bij de ingest)

"""
from collections import defaultdict
import json
import logging
from typing import Optional, Dict, List
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, String
from schematools import MAX_TABLE_LENGTH
from schematools.types import DatasetSchema
from schematools.utils import to_snake_case
from schematools.importer import fetch_col_type, get_table_name

metadata = MetaData()


logger = logging.getLogger(__name__)


def fetch_insert_data(event_data):
    return event_data["entity"]


def fetch_update_data(event_data):
    update_data = {}
    for modification in event_data["modifications"]:
        # XXX skip geometrie for now, has geojson format (no wkt)
        if modification["key"] == "geometrie":
            continue
        update_data[modification["key"]] = modification["new_value"]
    return update_data


EVENT_TYPE_MAPPING = {
    "ADD": ("insert", False, fetch_insert_data),
    "MODIFY": ("update", True, fetch_update_data),
    "DELETE": ("delete", True, None),
}

COLLECTION_TO_SCHEMA = {
    "gbd_bbk_gbd_brt_ligt_in_buurt": ("gebieden", "bouwblokken", "ligtInBuurt"),
    "gbd_ggp_gbd_brt_bestaat_uit_buurten": (
        "gebieden",
        "gebieden_ggwgebieden_bestaat_uit_buurten",
        "bestaat_uit_buurten",
    ),
}


class EventsProcessor:
    def __init__(
        self,
        datasets: List[DatasetSchema],
        srid,
        connection,
        local_metadata=None,
        truncate=False,
    ):
        self.datasets: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
        self.srid = srid
        self.conn = connection
        _metadata = local_metadata or metadata  # mainly for testing
        _metadata.bind = connection.engine
        self.tables = {}
        for dataset_id, dataset in self.datasets.items():
            base_tables_ids = set(dataset_table.id for dataset_table in dataset.tables)
            self.tables[dataset_id] = tfac = tables_factory(dataset, _metadata)
            self.geo_fields = defaultdict(lambda: defaultdict(list))
            for table_id, table in tfac.items():
                if not table.exists():
                    table.create()
                elif truncate:
                    self.conn.execute(table.delete())
                # self.has_compound_key = dataset_table.has_compound_key
                # skip the generated nm tables
                if table_id not in base_tables_ids:
                    continue
                for field in dataset.get_table_by_id(table_id).fields:
                    if field.is_geo:
                        self.geo_fields[dataset_id][table_id].append(field.name)

    def _fetch_relation_fieldnames(self, dataset_id, table_id, relation_fieldname):
        """We need the names of the fields in the relation.
        This is getting quite hairy.
        """
        # can also get this from identifier/temporal.identifier
        field = (
            self.datasets[dataset_id]
            .get_table_by_id(table_id)
            .get_field_by_id(relation_fieldname)
        )
        return sorted([to_snake_case(sf.name) for sf in field.sub_fields])

    def process_relation(self, source_id, event_data):

        event_type = event_data["_event_type"]
        _, _, data_fetcher = EVENT_TYPE_MAPPING[event_type]
        collection = event_data["_collection"]

        dataset_id, table_id, relation_fieldname = COLLECTION_TO_SCHEMA[collection]

        if data_fetcher is not None:
            row = data_fetcher(event_data)
            src_id = row["src_id"]
            src_volgnummer = row["src_volgnummer"]
            dst_id = row["dst_id"]
            dst_volgnummer = row["dst_volgnummer"]

            table = self.tables[dataset_id][table_id]
            id_fieldname, volgnummer_fieldname = self._fetch_relation_fieldnames(
                dataset_id, table_id, relation_fieldname
            )
            updates = {
                to_snake_case(f"{relation_fieldname}_id"): f"{dst_id}.{dst_volgnummer}",
                id_fieldname: dst_id,
                volgnummer_fieldname: dst_volgnummer,
            }

            # 1-n
            # ligt_in_wijk_id
            # ligt_in_wijk_identificatie
            # ligt_in_wijk_volgnummer

            # n-m
            # source_id
            # bestaat_uit_buurten_id
            # bestaat_uit_buurten_identificatie
            # bestaat_uit_buurten_volgnummer
            # ggwgebieden_id -> src_id.src_volgnummer
            # ggwgebieden_identificatie -> src_id
            # ggwgebieden_volgnummer -> src_volgnummer

            # andere where clause voor n-m (obv. de source_id)
            result = self.conn.execute(
                table.update()
                .where(table.c.id == f"{src_id}.{src_volgnummer}")
                .returning(table.c.id),
                updates,
            )
            if not result.fetchall():
                logger.warn(
                    "Nothing to update for %s-%s: %s.%s",
                    dataset_id,
                    table_id,
                    src_id,
                    src_volgnummer,
                )

    def process_row(self, source_id, event_data):

        event_type = event_data["_event_type"]
        db_operation_name, needs_select, data_fetcher = EVENT_TYPE_MAPPING[event_type]
        row = {}
        dataset_id = event_data["_catalog"]
        table_id = event_data["_collection"]
        identifier = self.datasets[dataset_id].get_table_by_id(table_id).identifier
        if data_fetcher is not None:
            row = data_fetcher(event_data)
            for field_name in self.geo_fields[dataset_id][table_id]:
                geo_value = row.get(field_name)
                if geo_value is not None:
                    row[field_name] = f"SRID={self.srid};{geo_value}"

            # Only for ADD we need to generate the PK
            if event_type == "ADD":
                id_value = ".".join(str(row[fn]) for fn in identifier)
                row["id"] = id_value
            row["source_id"] = source_id

        table = self.tables[dataset_id][table_id]
        db_operation = getattr(table, db_operation_name)()
        if needs_select:
            db_operation = db_operation.where(table.c.source_id == source_id)
        self.conn.execute(db_operation, row)

    def process_event(self, source_id, event_data, is_relation=False):
        """ Do inserts/updates/deletes """

        if is_relation:
            self.process_relation(source_id, event_data)
        else:
            self.process_row(source_id, event_data)

    def load_events_from_file(self, events_path, is_relation=False):
        """ Helper method, primarily used for testing """

        with open(events_path) as ef:
            for line in ef:
                if line.strip():
                    source_id, data_str = line.split("|", maxsplit=1)
                    event_data = json.loads(data_str)
                    self.process_event(
                        source_id,
                        event_data,
                        is_relation,
                    )


def tables_factory(
    dataset: DatasetSchema,
    metadata: Optional[MetaData] = None,
) -> Dict[str, Table]:
    """Generate thie SQLAlchemy Table objects to work with the JSON Schema

    :param dataset: The Amsterdam Schema definition of the dataset
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.

    The returned tables are keyed on the name of the dataset and table.
    The same goes for the incoming data, so during creation or records,
    the data can be associated with the correct table.
    """

    tables = defaultdict(dict)
    metadata = metadata or MetaData()

    for dataset_table in dataset.tables:
        db_table_name = get_table_name(dataset_table)
        table_id = dataset_table.id
        # Always add source_id
        columns = [
            Column("source_id", String, index=True, unique=True),
        ]
        sub_tables = {}
        for field in dataset_table.fields:
            if field.type.endswith("#/definitions/schema"):
                continue
            field_name = to_snake_case(field.name)
            sub_table_id = f"{db_table_name}_{field_name}"[:MAX_TABLE_LENGTH]

            try:

                if field.is_array:

                    if field.is_nested_table:
                        # We assume parent has an id field, Django needs it
                        fk_column = f"{db_table_name}.id"
                        sub_columns = [
                            Column("id", Integer, primary_key=True),
                            Column(
                                "parent_id", ForeignKey(fk_column, ondelete="CASCADE")
                            ),
                        ]

                    elif field.is_through_table:
                        # We need a 'through' table for the n-m relation
                        # these tables have a source_id as PK
                        sub_columns = [
                            Column("source_id", String, primary_key=True),
                            Column(
                                f"{dataset_table.id}_id",
                                String,
                            ),
                            Column(
                                f"{field_name}_id",
                                String,
                            ),
                        ]
                        # And the field(s) for the left side of the relation
                        # if this left table has a compound key
                        if dataset_table.has_compound_key:
                            for id_field in dataset_table.get_fields_by_id(
                                dataset_table.identifier
                            ):
                                sub_columns.append(
                                    Column(
                                        f"{dataset_table.id}_{to_snake_case(id_field.name)}",
                                        fetch_col_type(id_field),
                                    )
                                )

                    for sub_field in field.sub_fields:
                        colname_prefix = (
                            f"{field_name}_" if field.is_through_table else ""
                        )
                        sub_columns.append(
                            Column(
                                f"{colname_prefix}{to_snake_case(sub_field.name)}",
                                fetch_col_type(sub_field),
                            )
                        )

                    sub_tables[sub_table_id] = Table(
                        sub_table_id,
                        metadata,
                        *sub_columns,
                    )

                    continue

                col_type = fetch_col_type(field)
            except KeyError:
                raise NotImplementedError(
                    f'Import failed at "{field.name}": {dict(field)!r}\n'
                    f"Field type '{field.type}' is not implemented."
                ) from None

            col_kwargs = {"nullable": not field.required}
            if field.is_primary:
                col_kwargs["primary_key"] = True
                col_kwargs["nullable"] = False
                col_kwargs["autoincrement"] = False

            id_postfix = "_id" if field.relation else ""
            columns.append(Column(f"{field_name}{id_postfix}", col_type, **col_kwargs))

        tables[table_id] = Table(
            db_table_name, metadata, *columns, extend_existing=True
        )
        tables.update(**sub_tables)

    return tables
