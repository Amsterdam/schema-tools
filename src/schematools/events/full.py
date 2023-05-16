"""Module implementing an event processor, that processes full events."""
from __future__ import annotations

import logging
from collections import defaultdict

import orjson
from sqlalchemy import Table, inspect
from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.factories import tables_factory
from schematools.importer.base import BaseImporter
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema, DatasetTableSchema

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

FULL_LOAD_TABLE_POSTFIX = "_full_load"


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
        datasets: list[DatasetSchema],
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
        self.datasets: dict[str, DatasetSchema] = {ds.id: ds for ds in datasets}
        self.conn = connection
        _metadata = local_metadata or metadata  # mainly for testing
        _metadata.bind = connection.engine
        inspector = inspect(connection.engine)
        self.tables = {}
        self.full_load_tables = defaultdict(dict)
        for dataset_id, dataset in self.datasets.items():
            base_tables_ids = {dataset_table.id for dataset_table in dataset.tables}
            self.tables[dataset_id] = tfac = {
                # As quick workaround to map table identifiers with the existing event streams,
                # the code of this file works with snake cased identifiers.
                # The remaining code (e.g. BaseImporter) already works directly with table.id.
                to_snake_case(table_id): table
                for table_id, table in tables_factory(dataset, metadata=_metadata).items()
            }
            self.geo_fields = defaultdict(lambda: defaultdict(list))
            for table_id, table in tfac.items():
                if not inspector.has_table(table.name):
                    table.create()
                elif truncate:
                    with self.conn.begin():
                        self.conn.execute(table.delete())
                # self.has_composite_key = dataset_table.has_composite_key
                # skip the generated nm tables
                if table_id not in base_tables_ids:
                    continue
                for field in dataset.get_table_by_id(table_id).fields:
                    if field.is_geo:
                        self.geo_fields[dataset_id][table_id].append(field)

    def _flatten_event_data(self, event_data: dict) -> dict:
        result = {}
        for k, v in event_data.items():
            if isinstance(v, dict):
                flattened = self._flatten_event_data(v)
                for k2, v2 in flattened.items():
                    result[f"{k}_{k2}"] = v2
            else:
                result[k] = v
        return result

    def _get_full_load_tables(
        self, dataset_id: str, table_id: str
    ) -> tuple[Table, DatasetTableSchema]:
        try:
            # Cached
            table, schema_table = self.full_load_tables[dataset_id][table_id]
        except KeyError:
            # Initialise fresh table for full load.
            dataset_schema = self.datasets[dataset_id]
            schema_table = dataset_schema.get_table_by_id(table_id)
            db_table_name = schema_table.db_name_variant(postfix=FULL_LOAD_TABLE_POSTFIX)
            importer = BaseImporter(dataset_schema, self.conn.engine, logger)
            importer.generate_db_objects(
                table_id,
                db_table_name=db_table_name,
                is_versioned_dataset=importer.is_versioned_dataset,
                ind_extra_index=False,
                ind_create_pk_lookup=False,
            )

            table = importer.tables[table_id]
            self.full_load_tables[dataset_id][table_id] = table, schema_table

        return table, schema_table

    def _before_process(self, event_meta: dict) -> tuple[Table, DatasetTableSchema]:
        dataset_id = event_meta["dataset_id"]
        table_id = event_meta["table_id"]

        if event_meta.get("full_load_sequence", False):
            table, schema_table = self._get_full_load_tables(dataset_id, table_id)

            if event_meta.get("first_of_sequence", False):
                self.conn.engine.execute(f"TRUNCATE {table.name}")

            return table, schema_table
        else:
            schema_table = self.datasets[dataset_id].get_table_by_id(table_id)
            table = self.tables[dataset_id][to_snake_case(table_id)]
            return table, schema_table

    def _after_process(self, event_meta: dict, table: Table):
        if event_meta.get("full_load_sequence", False) and event_meta.get(
            "last_of_sequence", False
        ):
            dataset_id = event_meta["dataset_id"]
            table_id = event_meta["table_id"]
            table_to_replace = self.tables[dataset_id][to_snake_case(table_id)]

            logger.info("End of full load sequence. Replacing active table.")
            with self.conn.begin():
                self.conn.execute(f"TRUNCATE {table_to_replace.name}")
                self.conn.execute(
                    f"INSERT INTO {table_to_replace.name} "  # nosec B608 # noqa: S608
                    f"SELECT * FROM {table.name}"  # nosec B608 # noqa: S608
                )
                self.conn.execute(f"DROP TABLE {table.name} CASCADE")
            self.full_load_tables[dataset_id].pop(table_id)

    def _prepare_row(
        self, event_meta: dict, event_data: dict, schema_table: DatasetTableSchema
    ) -> dict:
        dataset_id = event_meta["dataset_id"]
        table_id = event_meta["table_id"]

        row = self._flatten_event_data(event_data)

        for geo_field in self.geo_fields[dataset_id][table_id]:
            geo_value = row.get(geo_field.name)
            if geo_value is not None and not geo_value.startswith("SRID"):
                row[geo_field.name] = f"SRID={geo_field.srid};{geo_value}"

        identifier = schema_table.identifier
        id_value = ".".join(str(row[fn]) for fn in identifier)
        row["id"] = id_value
        return row

    def _process_row(
        self, event_meta: dict, event_data: dict, table: Table, schema_table: DatasetTableSchema
    ) -> None:
        """Process one row of data.

        Args:
            event_id: Id of the event (Kafka id)
            event_meta: Metadata about the event
            event_data: Data containing the fields of the event
        """

        row = self._prepare_row(event_meta, event_data, schema_table)
        id_value = row["id"]

        event_type = event_meta["event_type"]
        db_operation_name, needs_select = EVENT_TYPE_MAPPINGS[event_type]
        db_operation = getattr(table, db_operation_name)()

        update_parent_op = update_parent_row = None
        if schema_table.has_parent_table:
            dataset_id = event_meta["dataset_id"]
            # Have relation. We need to update the relation columns in the parent table as well
            rel_field_prefix = to_snake_case(schema_table.parent_table_field.name)
            parent_schema_table = schema_table.parent_table
            parent_table = self.tables[dataset_id][parent_schema_table.id]
            parent_id_field = (
                parent_table.c.id
                if parent_schema_table.has_composite_key
                else getattr(parent_table.c, parent_schema_table.identifier[0])
            )
            parent_id_value = ".".join(
                [
                    str(row[f"{parent_schema_table.id}_{fn}"])
                    for fn in parent_schema_table.identifier
                ]
            )

            update_parent_op = parent_table.update()
            update_parent_op = update_parent_op.where(parent_id_field == parent_id_value)
            update_parent_row = {
                k: v for k, v in event_data.items() if k.startswith(rel_field_prefix)
            }
            if event_type == "DELETE":
                update_parent_row = {k: None for k in update_parent_row.keys()}

        if needs_select:
            id_field = (
                table.c.id
                if schema_table.has_composite_key
                else getattr(table.c, schema_table.identifier[0])
            )
            db_operation = db_operation.where(id_field == id_value)
        with self.conn.begin():
            self.conn.execute(db_operation, row)

            if update_parent_op is not None:
                self.conn.execute(update_parent_op, update_parent_row)

    def process_event(self, event_meta: dict, event_data: dict):
        table, schema_table = self._before_process(event_meta)
        """Do inserts/updates/deletes."""
        self._process_row(event_meta, event_data, table, schema_table)
        self._after_process(event_meta, table)

    def process_events(self, events: list[tuple[dict, dict]]):
        if len(events) == 0:
            return

        first_event_meta = events[0][0]
        last_event_meta = events[-1][0]
        table, schema_table = self._before_process(first_event_meta)

        if first_event_meta.get("full_load_sequence", False):
            # full_load_sequence only contains add events. Take more efficient shortcut.
            self._process_bulk_adds(events, table, schema_table)
        else:
            for event_meta, event_data in events:
                self._process_row(event_meta, event_data, table, schema_table)

        self._after_process(last_event_meta, table)

    def _process_bulk_adds(self, events: list[tuple[dict, dict]], table, schema_table):
        rows = []
        for event_meta, event_data in events:
            if event_meta["event_type"] != "ADD":
                raise Exception("This method should only be called when processing ADD events.")

            row = self._prepare_row(event_meta, event_data, schema_table)
            rows.append(row)

        with self.conn.begin():
            self.conn.execute(table.insert(), rows)

    def load_events_from_file(self, events_path: str):
        """Load events from a file, primarily used for testing."""
        with open(events_path, "rb") as ef:
            for line in ef:
                if line := line.strip():
                    event_id, event_meta_str, data_str = line.split(b"|", maxsplit=2)
                    event_meta = orjson.loads(event_meta_str)
                    event_data = orjson.loads(data_str)
                    self.process_event(
                        event_meta,
                        event_data,
                    )

    def load_events_from_file_using_bulk(self, events_path: str):
        """Load events from a file, primarily used for testing."""
        with open(events_path, "rb") as ef:
            events = []
            for line in ef:
                if line := line.strip():
                    event_id, event_meta_str, data_str = line.split(b"|", maxsplit=2)
                    event_meta = orjson.loads(event_meta_str)
                    event_data = orjson.loads(data_str)
                    events.append((event_meta, event_data))
            self.process_events(events)
