"""Module implementing an event processor, that processes full events."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Dict, List

from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.events.factories import tables_factory
from schematools.types import DatasetSchema
from schematools.utils import to_snake_case, toCamelCase

# Enable the sqlalchemy logger by uncommenting the following 2 lines to debug SQL related issues
# logging.basicConfig()
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


# Configuration information to map the event-type
# to the following fields:
# - db_operation_name (INSERT/UPDATE)
# - needs_select: generated SQL needs to select a record

EVENT_TYPE_MAPPINGS = {
    "ADD": ("insert", False),
    "MODIFY": ("update", True),
    "DELETE": ("delete", True),
}


class DataSplitter:
    """Helper class to split a data event record.

    Data is split into scalar fields and relational fields.
    Scalar data can be used directly to provide SQL queries with data.
    Relational data is used to update records with FK colums, or, for NM relations,
    to construct SQL statement to update junction tables.
    """

    def __init__(
        self, events_processor: EventsProcessor, dataset_id: str, table_id: str, event_data: dict
    ) -> None:
        """Construct the DataSplitter.

        Args:
            events_processor: reference to the EventsProcessor, usually,
                this is a backref. where the EventsProcessor is instantiating the DataSplitter
            dataset_id: identifier of the dataset
            table_id: identifier of the table
            event_data: the actual event data
        """
        self.events_processor = events_processor
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.dataset_table = events_processor.datasets[dataset_id].get_table_by_id(table_id)
        self.junction_table_rows = {}
        relational_data, self.row_data = self._split_data(event_data)
        self.process_relational_data(relational_data)

    def _split_data(self, event_data: dict) -> list[dict]:
        """Split the event_data in 2 parts, one with scalars, the other with relations."""
        data_bags = [{}, {}]
        for field in self.dataset_table.fields:
            snaked_field_id = to_snake_case(field.id)
            if snaked_field_id in event_data:
                data_bags[field.is_scalar][snaked_field_id] = event_data[snaked_field_id]
        return data_bags

    def process_relational_data(self, relational_data: dict) -> None:
        """Process the relational_data.

        Determines if relation is FK or NM. Handles appropriately.
        """
        for field_id, field_data in relational_data.items():
            field = self.dataset_table.get_field_by_id(toCamelCase(field_id))
            if field.relation is not None:
                self._handle_fk(field_id, field_data, field.relation)
                field_data = [] if field_data is None else [field_data]
                self._handle_junction_table(field_id, field_data, field.relation)
            elif field.nm_relation is not None:
                self._handle_junction_table(field_id, field_data, field.nm_relation)
            else:
                raise Exception("Relation should be either FK or NM")

    def fetch_row(self) -> dict:
        """Fetch the row data.

        To be used in a SQL update or insert query.
        """
        return self.row_data

    def _handle_fk(self, field_id: str, field_data: dict, relation: str):
        """Handle FK relation."""
        fk_row_data = {}
        snaked_field_id = to_snake_case(field_id)
        target_identifier_fields = self._fetch_target_identifier_fields(relation)

        # id for target side of relation
        if field_data:
            id_value = ".".join((str(field_data[fn]) for fn in target_identifier_fields))
        else:
            id_value = None

        fk_row_data[f"{snaked_field_id}_id"] = id_value
        for fn in self._fetch_target_identifier_fields(relation):
            fk_row_data[f"{snaked_field_id}_{fn}"] = (field_data or {}).get(fn)

        self.row_data.update(fk_row_data)

    def _fetch_target_identifier_fields(self, relation: str) -> list[str]:
        """Fetch the identifier fields for the target side of a relation.

        These fields will be ordered with the main identifier first, and
        on the second position the sequence identifier.
        """
        dataset_id, table_id = relation.split(":")
        target_dataset = self.events_processor.datasets[dataset_id]
        target_dataset_table = target_dataset.get_table_by_id(table_id)
        return target_dataset_table.identifier

    def _fetch_source_id_info(self):
        # id info for source side of relation (for updates/deletes)
        snaked_source_table = to_snake_case(self.table_id)
        identifier = self.dataset_table.identifier
        id_part_values = {fn: self.row_data[fn] for fn in identifier}
        id_value = ".".join(str(part) for part in id_part_values.values())
        return (f"{snaked_source_table}_id", id_value, id_part_values)

    def _handle_junction_table(self, field_id: str, field_data: dict, nm_relation):
        """Handle NM relation."""
        snaked_field_id = to_snake_case(field_id)
        target_identifier_fields = self._fetch_target_identifier_fields(nm_relation)
        snaked_source_table_id, id_value, id_part_values = self._fetch_source_id_info()

        # Short circuit when no data
        if not field_data:
            self.junction_table_rows[snaked_field_id] = []
            return

        nm_rows = []

        for row_data in field_data:
            nm_row_data = {}

            # relation fields for source side
            # XXX add support for shortnames!
            for sub_field_schema in self.dataset_table.get_fields_by_id(
                *self.dataset_table.identifier
            ):
                sub_field_id = f"{self.table_id}_{sub_field_schema.id}"
                nm_row_data[sub_field_id] = id_part_values[sub_field_schema.id]

            # relation fields for target side
            for fn, fv in row_data.items():
                name_prefix = f"{snaked_field_id}_" if fn in target_identifier_fields else ""
                nm_row_data[f"{name_prefix}{fn}"] = fv

            # id for source side of relation
            nm_row_data.update({snaked_source_table_id: id_value})

            # id for target side of relation
            nm_row_data[f"{snaked_field_id}_id"] = ".".join(
                (str(row_data[fn]) for fn in target_identifier_fields)
            )
            nm_rows.append(nm_row_data)

        self.junction_table_rows[snaked_field_id] = nm_rows

    def update_relations(self):
        """Perform SQL calls to update the relations.

        Update is performed by first deleting all records pointing
        to the source table, and then re-inserting those records.

        Assertion is that GOB always provides fully populated records.
        """
        conn = self.events_processor.conn

        with conn.begin():
            for snaked_field_id, nm_row_data in self.junction_table_rows.items():

                snaked_source_table_id = to_snake_case(self.table_id)
                junction_table_id = f"{snaked_source_table_id}_{snaked_field_id}"
                sa_table = self.events_processor.tables[self.dataset_id][junction_table_id]
                source_id, source_id_value, _ = self._fetch_source_id_info()

                source_id_column = getattr(sa_table.c, source_id)
                conn.execute(sa_table.delete().where(source_id_column == source_id_value))
                if nm_row_data:
                    conn.execute(sa_table.insert(), nm_row_data)


class EventsProcessor:
    """The core event processing class.

    It needs to be initialised once
    with configuration (datasets) and a db connection.
    Once initialised, the process_event() method is able to
    process incoming events.
    The database actions are done using SQLAlchemy Core. So,
    a helper function `tables_factory()` is used to created the
    SA Tables that are needed during the processing of the events.
    """

    def __init__(
        self,
        datasets: List[DatasetSchema],
        srid: str,
        connection: Connection,
        local_metadata=None,
        truncate=False,
    ) -> None:
        """Construct the event processor.

        Args:
            datasets: list of DatasetSchema instances. Usually dataset tables
                have relations to tables.
                If these target tables are in different datasets,
                these dataset also need to be provided.
            srid: coordinate system
            local_metadata: SQLAlchemy metadata object, only needs to be provided
                in unit tests.
            truncate: indicates if the relational tables need to be truncated
        """
        self.datasets: Dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
        self.srid = srid
        self.conn = connection
        _metadata = local_metadata or metadata  # mainly for testing
        _metadata.bind = connection.engine
        self.tables = {}
        for dataset_id, dataset in self.datasets.items():
            base_tables_ids = {dataset_table.id for dataset_table in dataset.tables}
            self.tables[dataset_id] = tfac = tables_factory(dataset, _metadata)
            self.geo_fields = defaultdict(lambda: defaultdict(list))
            for table_id, table in tfac.items():
                if not table.exists():
                    table.create()
                elif truncate:
                    with self.conn.begin():
                        self.conn.execute(table.delete())
                # self.has_compound_key = dataset_table.has_compound_key
                # skip the generated nm tables
                if table_id not in base_tables_ids:
                    continue
                for field in dataset.get_table_by_id(table_id).fields:
                    if field.is_geo:
                        self.geo_fields[dataset_id][table_id].append(field.name)

    def process_row(self, event_id: str, event_meta: dict, event_data: dict) -> None:
        """Process one row of data.

        Args:
            event_id: Id of the event (Kafka id)
            event_meta: Metadata about the event
            event_data: Data containing the fields of the event
        """
        event_type = event_meta["event_type"]
        db_operation_name, needs_select = EVENT_TYPE_MAPPINGS[event_type]
        dataset_id = event_meta["dataset_id"]
        table_id = event_meta["table_id"]
        data_splitter = DataSplitter(self, dataset_id, table_id, event_data)

        row = data_splitter.fetch_row()

        for field_name in self.geo_fields[dataset_id][table_id]:
            geo_value = row.get(field_name)
            if geo_value is not None and not row[field_name].startswith("SRID"):
                row[field_name] = f"SRID={self.srid};{geo_value}"

        identifier = self.datasets[dataset_id].get_table_by_id(table_id).identifier
        id_value = ".".join(str(row[fn]) for fn in identifier)
        row["id"] = id_value

        table = self.tables[dataset_id][table_id]
        db_operation = getattr(table, db_operation_name)()
        if needs_select:
            # XXX Can we assume 'id' is always available?
            db_operation = db_operation.where(table.c.id == id_value)
        with self.conn.begin():
            self.conn.execute(db_operation, row)

        # now process the relations (if any)
        data_splitter.update_relations()

    def process_event(self, event_id: str, event_meta: dict, event_data: dict):
        """Do inserts/updates/deletes."""
        self.process_row(event_id, event_meta, event_data)

    def load_events_from_file(self, events_path: str):
        """Load events from a file, primarily used for testing."""
        with open(events_path) as ef:
            for line in ef:
                if line.strip():
                    event_id, event_meta_str, data_str = line.split("|", maxsplit=2)
                    event_meta = json.loads(event_meta_str)
                    event_data = json.loads(data_str)
                    self.process_event(
                        event_id,
                        event_meta,
                        event_data,
                    )
