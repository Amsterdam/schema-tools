__doc__ = """

Direct de tabel bijwerken, of via een tussenstap (met validaties)
Validaties wschl. eerder in het proces (bij de ingest)

"""
from collections import defaultdict
from dataclasses import dataclass
import json
import logging
from typing import Optional, Callable, Dict, List
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, String
from schematools import MAX_TABLE_LENGTH
from schematools.types import DatasetSchema
from schematools.utils import to_snake_case
from schematools.importer import fetch_col_type, get_table_name

metadata = MetaData()

# Enable the sqlalchemy logger to debug SQL related issues
# logging.basicConfig()
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


class UnknownRelationException(Exception):
    """Custom Exception to signal the fact that an incoming
    event is unsing an unknown type of relation for
    which we do not have configuration info available.
    """

    pass


def fetch_insert_data(event_data):
    # remove dict values from the event. We only handle scalar datatypes.
    # Json blobs in the events are containing irrelevant GOB-only data.
    return {k: v for k, v in event_data["entity"].items() if not isinstance(v, dict)}


def fetch_update_data(event_data):
    # Update data has a different structure in the events. We convert it
    # into a single dict (like for insert data), so we can handle
    # the data in the exact same way.
    update_data = {}
    for modification in event_data["modifications"]:
        # XXX skip geometrie for now, has geojson format (should be wkt)
        if modification["key"] == "geometrie":
            continue
        update_data[modification["key"]] = modification["new_value"]
    return update_data


FK_TABLE = False
NM_TABLE = True  # nm_table_id is not None


# Configuration information to map the event-type
# to the following fields (package into a DbInfo dataclass):
# - db_operation_name (INSERT/UPDATE)
# - needs_select: generated SQL needs to select a record
# - needs_values: generated SQL needs values (to insert/update)
# - data_fetcher: function to be used to extract data from the event

EVENT_TYPE_MAPPINGS = {
    FK_TABLE: {
        "ADD": ("update", True, True, fetch_insert_data),
        "MODIFY": ("update", True, True, fetch_update_data),
        "DELETE": ("update", True, True, None),
    },
    NM_TABLE: {
        "ADD": ("insert", False, True, fetch_insert_data),
        "MODIFY": ("update", True, True, fetch_update_data),
        "DELETE": ("delete", True, False, None),
    },
}


@dataclass
class DbInfo:
    db_operation_name: str
    needs_select: bool
    needs_values: bool
    data_fetcher: Optional[Callable[[Dict], Dict]]

    @classmethod
    def from_event_type(cls, is_nm_table: bool, event_type: str):
        return cls(*EVENT_TYPE_MAPPINGS[is_nm_table][event_type])


# Translation table of GOB relation names to amsterdam schema configuration
# Events for relations contain a 'rel' field that is an internal GOB name
# We need to associate configuration info with these events to be able
# to map the event to the correct tables according to the amsterdam schema.

# Structure
# <gob rel name> :
#       (<amschema dataset>, <amschema table>, <nm relation table>, <amschema fieldname>)

COLLECTION_TO_SCHEMA = {
    "gbd_bbk_gbd_brt_ligt_in_buurt": ("gebieden", "bouwblokken", None, "ligt_in_buurt"),
    "gbd_brt_gbd_wijk_ligt_in_wijk": ("gebieden", "buurten", None, "ligt_in_wijk"),
    "gbd_ggp_gbd_sdl_ligt_in_stadsdeel": (
        "gebieden",
        "ggpgebieden",
        None,
        "ligt_in_stadsdeel",
    ),
    "gbd_ggw_gbd_sdl_ligt_in_stadsdeel": (
        "gebieden",
        "ggwgebieden",
        None,
        "ligt_in_stadsdeel",
    ),
    "gbd_wijk_gbd_sdl_ligt_in_stadsdeel": (
        "gebieden",
        "wijken",
        None,
        "ligt_in_stadsdeel",
    ),
    "gbd_ggw_gbd_brt_bestaat_uit_buurten": (
        "gebieden",
        "ggwgebieden",
        "gebieden_ggwgebieden_bestaat_uit_buurten",
        "bestaat_uit_buurten",
    ),
    "gbd_ggp_gbd_brt_bestaat_uit_buurten": (
        "gebieden",
        "ggpgebieden",
        "gebieden_ggpgebieden_bestaat_uit_buurten",
        "bestaat_uit_buurten",
    ),
}

# Special fields, unable to query
# gbd_brt_gbd_ggp__ligt_in_ggpgebied
# gbd_brt_gbd_ggw__ligt_in_ggwgebied
# gbd_wijk_gbd_ggw__ligt_in_ggwgebied

# Relatie buiten gebieden
# gbd_sdl_brk_gme_ligt_in_gemeente


@dataclass
class SchemaInfo:
    dataset_id: str
    table_id: str
    nm_table_id: str
    relation_fieldname: str
    use_dimension_fields: bool = False

    @classmethod
    def from_collection(cls, collection, datasets):
        try:
            schema_data = COLLECTION_TO_SCHEMA[collection]
        except KeyError as e:
            raise UnknownRelationException(
                f"Relation {collection} cannot be handled"
            ) from e
        schema_info = cls(*schema_data)
        schema_info.use_dimension_fields = datasets[
            schema_info.dataset_id
        ].use_dimension_fields
        return schema_info


# Mapping of GOB temporal relation names to amsterdam schema names

GOB_CORE_FIELD_NAMES = {
    "src_id": "identificatie",
    "src_volgnummer": "volgnummer",
    "dst_id": "identificatie",
    "dst_volgnummer": "volgnummer",
}

GOB_DIMENSION_FIELD_NAMES = {
    "begin_geldigheid": "begin_geldigheid",
    "eind_geldigheid": "eind_geldigheid",
}


class RelationHandler:
    """Specialised handler class to work with relation events.
    For an incoming relation, a records in the database need to be inserted/updated/deleted.
    This handler (and its specialised subclasses) can handle this process in the following way:
    - determine the exact type of handler
    - instantiate it (giving it the configuration and data to work with)
    - call set_query_info()  (associated columns, where clause)
    - call set_value()  (values needed during execution of the query)
    - call execute()  (execute the query)
    """

    def __init__(self, source_id, event_data, event_type, tables, db_info, schema_info):
        self.source_id = source_id
        self.event_type = event_type
        self.tables = tables
        self.db_info = db_info
        self.schema_info = schema_info
        self.row = {}
        if db_info.data_fetcher is not None:
            self.row = db_info.data_fetcher(event_data)
        self.gob_field_names = GOB_CORE_FIELD_NAMES.copy()
        if self.schema_info.use_dimension_fields:
            self.gob_field_names.update(GOB_DIMENSION_FIELD_NAMES)
        for fn in self.gob_field_names.keys():
            setattr(self, fn, self.row.get(fn))

    def _add_update(self, initial, fn, value, prefix=None):
        if fn in self.gob_field_names.keys() and self.row.get(fn) is not None:
            dso_fn = self.gob_field_names[fn]
            initial[
                "_".join(([prefix] if prefix is not None else []) + [dso_fn])
            ] = value

    @classmethod
    def fetch_handler(cls, is_nm_table):
        # Factory that returns the correct subclass
        return [FKRelationHandler, M2MRelationHandler][is_nm_table]

    def set_query_info(self):
        self.table = self.tables[self.schema_info.dataset_id][self.schema_info.table_id]

    def set_values(self):
        relation_fieldname = self.schema_info.relation_fieldname
        updates = {}
        id_value = (
            None if self.dst_id is None else f"{self.dst_id}.{self.dst_volgnummer}"
        )
        if id_value is not None:
            updates = {f"{relation_fieldname}_id": id_value}
        self._add_update(updates, "dst_id", self.dst_id, relation_fieldname)
        self._add_update(
            updates, "dst_volgnummer", self.dst_volgnummer, relation_fieldname
        )
        self.updates = updates

    def execute(self, conn):
        db_operation = getattr(self.table, self.db_info.db_operation_name)()
        if self.db_info.needs_select:
            db_operation = db_operation.where(self.where_clause)
        if self.db_info.needs_values:
            if not self.updates:
                logger.warn("No values for update: %s", self.row)
                return
            db_operation = db_operation.values(**self.updates)
        result = conn.execute(
            db_operation.returning(self.column),
        )
        retval = result.fetchall()
        if not retval:
            logger.warn(
                "Nothing to update for %s",
                self.row,
            )


class FKRelationHandler(RelationHandler):
    """ Specialised subclass for FK relations """

    def set_query_info(self):
        super().set_query_info()
        if self.event_type == "ADD":
            self.column = self.table.c.id
            self.where_clause = self.column == f"{self.src_id}.{self.src_volgnummer}"
        else:
            self.column = self.table.c[
                f"{self.schema_info.relation_fieldname}_source_id"
            ]
            self.where_clause = self.column == self.source_id

    def _null_updates(self):
        relation_fieldname = self.schema_info.relation_fieldname
        updates = {f"{relation_fieldname}_id": None}
        for fn in set(self.gob_field_names.values()):
            # The src fields are not used for FK, only during ADD
            if fn.startswith("src"):
                continue
            updates[f"{relation_fieldname}_{fn}"] = None
        return updates

    def set_values(self):
        if self.event_type == "DELETE":
            self.updates = self._null_updates()
        else:
            super().set_values()
            relation_fieldname = self.schema_info.relation_fieldname
            updates = {f"{relation_fieldname}_source_id": self.source_id}
            if self.schema_info.use_dimension_fields:
                self._add_update(
                    updates,
                    "begin_geldigheid",
                    self.begin_geldigheid,
                    relation_fieldname,
                )
                self._add_update(
                    updates, "eind_geldigheid", self.eind_geldigheid, relation_fieldname
                )
            self.updates.update(updates)


class M2MRelationHandler(RelationHandler):
    """ Specialised subclass for M2M relations """

    def set_query_info(self):
        # Need the NM table
        self.table = self.tables[self.schema_info.dataset_id][
            self.schema_info.nm_table_id
        ]
        self.column = self.table.c["source_id"]
        self.where_clause = self.column == self.source_id

    def set_values(self):
        super().set_values()
        table_id = self.schema_info.table_id
        updates = {}
        id_value = (
            None if self.src_id is None else f"{self.src_id}.{self.src_volgnummer}"
        )
        if id_value is not None:
            updates = {
                "source_id": self.source_id,
                f"{table_id}_id": id_value,
            }
        self._add_update(updates, "src_id", self.src_id, table_id)
        self._add_update(updates, "src_volgnummer", self.src_volgnummer, table_id)
        if self.schema_info.use_dimension_fields:
            self._add_update(updates, "begin_geldigheid", self.begin_geldigheid)
            self._add_update(updates, "eind_geldigheid", self.eind_geldigheid)
        self.updates.update(updates)


class EventsProcessor:
    """The core event processing class. It needs to be initialised once
    with configuration (datasets) and a db connection.
    Once initialised, the process_event() method is able to
    process incoming events.
    The database actions are done using SQLAlchemy Core. So,
    a helper function tables_factory() is used to created the
    SA Tables that are needed during the processing of the events.
    """

    def __init__(
        self,
        datasets: List[DatasetSchema],
        srid,
        connection,
        local_metadata=None,
        truncate=False,
    ):
        self.datasets: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
        for ds in self.datasets.values():
            ds.add_dataset_to_cache(ds)
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

    def process_relation(self, source_id, event_meta, event_data):
        collection = event_meta["collection"]
        schema_info = SchemaInfo.from_collection(collection, self.datasets)
        event_type = event_meta["event_type"]
        is_nm_table = schema_info.nm_table_id is not None
        db_info = DbInfo.from_event_type(is_nm_table, event_type)

        relation_handler = RelationHandler.fetch_handler(is_nm_table)(
            source_id, event_data, event_type, self.tables, db_info, schema_info
        )

        relation_handler.set_query_info()
        relation_handler.set_values()
        relation_handler.execute(self.conn)

    def process_row(self, source_id, event_meta, event_data):

        event_type = event_meta["event_type"]
        db_operation_name, needs_select, _, data_fetcher = EVENT_TYPE_MAPPINGS[
            NM_TABLE
        ][event_type]
        row = {}
        dataset_id = event_meta["catalog"]
        table_id = event_meta["collection"]
        if data_fetcher is not None:
            row = data_fetcher(event_data)
            for field_name in self.geo_fields[dataset_id][table_id]:
                geo_value = row.get(field_name)
                if geo_value is not None:
                    row[field_name] = f"SRID={self.srid};{geo_value}"

            # Only for ADD we need to generate the PK
            if event_type == "ADD":
                identifier = (
                    self.datasets[dataset_id].get_table_by_id(table_id).identifier
                )
                id_value = ".".join(str(row[fn]) for fn in identifier)
                row["id"] = id_value
            row["source_id"] = source_id

        table = self.tables[dataset_id][table_id]
        db_operation = getattr(table, db_operation_name)()
        if needs_select:
            db_operation = db_operation.where(table.c.source_id == source_id)
        self.conn.execute(db_operation, row)

    def process_event(self, source_id, event_meta, event_data, is_relation=False):
        """ Do inserts/updates/deletes """

        if is_relation:
            try:
                self.process_relation(source_id, event_meta, event_data)
            except UnknownRelationException as e:
                logger.warn("Unknown Relation: %s", e)
        else:
            self.process_row(source_id, event_meta, event_data)

    def load_events_from_file(self, events_path):
        """ Helper method, primarily used for testing """

        with open(events_path) as ef:
            for line in ef:
                if line.strip():
                    source_id, event_meta_str, data_str = line.split("|", maxsplit=2)
                    event_meta = json.loads(event_meta_str)
                    is_relation = event_meta["catalog"] == "rel"
                    event_data = json.loads(data_str)
                    self.process_event(
                        source_id,
                        event_meta,
                        event_data,
                        is_relation,
                    )


def tables_factory(
    dataset: DatasetSchema,
    metadata: Optional[MetaData] = None,
) -> Dict[str, Table]:
    """Generate the SQLAlchemy Table objects to work with the JSON Schema

    :param dataset: The Amsterdam Schema definition of the dataset
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.

    The returned tables are keyed on the name of the dataset and table.
    SA Table objects are also created for the NM-relation tables.
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
            sub_columns = []

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
                        # And two FK fields for both sides of the relation
                        # containing a concatenation of two fields
                        # usually identificatie and volgnummer
                        sub_columns = [
                            Column("source_id", String, primary_key=True),
                            Column(
                                f"{dataset_table.id}_id",
                                String,
                                index=True,
                            ),
                            Column(
                                f"{field_name}_id",
                                String,
                                index=True,
                            ),
                        ]
                        # And the field(s) for the left side of the relation
                        # if this left table has a compound key
                        # Alternative would be to move this to sub_fields method in
                        # the types module.
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
                        sub_columns.append(
                            Column(
                                f"{to_snake_case(sub_field.name)}",
                                fetch_col_type(sub_field),
                            )
                        )

                    sub_tables[sub_table_id] = Table(
                        sub_table_id, metadata, *sub_columns, extend_existing=True
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

            # We need an extra field source_id for 1-n relations
            # This should not be part of the schema, so only generate it here
            # and not in the table.sub_fields (types.py)
            if field.relation is not None:
                columns.append(
                    Column(f"{field_name}_source_id", String, index=True, unique=True)
                )

        tables[table_id] = Table(
            db_table_name, metadata, *columns, extend_existing=True
        )
        tables.update(**sub_tables)

    return tables
