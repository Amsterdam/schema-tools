"""Module implementing an event processor, that processes full events."""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass

import orjson
from sqlalchemy import Table, inspect
from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.factories import tables_factory
from schematools.importer.base import BaseImporter
from schematools.loaders import get_schema_loader
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


@dataclass
class RunConfiguration:
    check_existence_on_add = False
    process_events = True
    execute_after_process = True

    table = None
    schema_table = None


class LastEventIds:
    BENK_DATASET = "benk"
    LASTEVENTIDS_TABLE = "lasteventids"

    def __init__(self):
        loader = get_schema_loader()
        self.dataset = loader.get_dataset(self.BENK_DATASET)
        self.table = self.dataset.get_table_by_id(self.LASTEVENTIDS_TABLE)
        self.lasteventid_column = self.table.get_field_by_id("lastEventId")

    def is_event_processed(self, conn, schema_table: DatasetTableSchema, event_id: int) -> bool:
        res = conn.execute(
            f"SELECT {self.lasteventid_column.db_name} FROM {self.table.db_name} "  # noqa: S608
            f"WHERE \"table\" = '{schema_table.db_name}'"  # noqa: S608
        ).fetchone()

        return False if res is None else res[0] >= event_id

    def update_eventid(self, conn, schema_table: DatasetTableSchema, event_id: int):
        conn.execute(
            f"INSERT INTO {self.table.db_name} "  # noqa: S608  # nosec: B608
            f"VALUES ('{schema_table.db_name}', {event_id}) "
            f'ON CONFLICT ("table") DO UPDATE SET {self.lasteventid_column.db_name} = {event_id}'
        )


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
        self.lasteventids = LastEventIds()
        benk_dataset = self.lasteventids.table.dataset
        self.datasets = {benk_dataset.id: benk_dataset} | {ds.id: ds for ds in datasets}
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

    def _before_process(self, run_configuration: RunConfiguration, event_meta: dict) -> None:
        if event_meta.get("full_load_sequence", False):

            if event_meta.get("first_of_sequence", False):
                self.conn.execute(f"TRUNCATE {run_configuration.table.name}")

    def _after_process(self, run_configuration: RunConfiguration, event_meta: dict):
        if event_meta.get("full_load_sequence", False) and event_meta.get(
            "last_of_sequence", False
        ):
            dataset_id = event_meta["dataset_id"]
            table_id = event_meta["table_id"]
            table_to_replace = self.tables[dataset_id][to_snake_case(table_id)]

            fieldnames = ", ".join(
                [field.db_name for field in run_configuration.schema_table.get_db_fields()]
            )

            logger.info("End of full load sequence. Replacing active table.")
            with self.conn.begin():
                self.conn.execute(f"TRUNCATE {table_to_replace.name}")
                self.conn.execute(
                    f"INSERT INTO {table_to_replace.name} ({fieldnames}) "  # noqa: S608
                    f"SELECT {fieldnames} FROM {run_configuration.table.name}"  # noqa: S608
                )
                self.conn.execute(f"DROP TABLE {run_configuration.table.name} CASCADE")
            self.full_load_tables[dataset_id].pop(table_id)

    def _prepare_row(
        self, event_meta: dict, event_data: dict, schema_table: DatasetTableSchema
    ) -> dict:
        dataset_id = event_meta["dataset_id"]
        table_id = event_meta["table_id"]

        row = self._flatten_event_data(event_data)

        for geo_field in self.geo_fields[dataset_id][table_id]:
            row_key = to_snake_case(geo_field.name)
            geo_value = row.get(row_key)
            if geo_value is not None and not geo_value.startswith("SRID"):
                row[row_key] = f"SRID={geo_field.srid};{geo_value}"

        identifier = schema_table.identifier
        id_value = ".".join(str(row[to_snake_case(fn)]) for fn in identifier)
        row["id"] = id_value
        return row

    def _process_row(
        self, run_configuration: RunConfiguration, event_meta: dict, event_data: dict
    ) -> None:
        """Process one row of data.

        Args:
            event_id: Id of the event (Kafka id)
            event_meta: Metadata about the event
            event_data: Data containing the fields of the event
        """
        table = run_configuration.table
        schema_table = run_configuration.schema_table

        if self.lasteventids.is_event_processed(self.conn, schema_table, event_meta["event_id"]):
            logger.warning("Event with id %s already processed. Skipping.", event_meta["event_id"])
            return

        row = self._prepare_row(event_meta, event_data, schema_table)
        id_value = row["id"]

        event_type = event_meta["event_type"]

        if (
            run_configuration.check_existence_on_add
            and event_type == "ADD"
            and self._row_exists_in_database(run_configuration, id_value)
        ):
            logger.info("Row with id %s already exists in database. Skipping.", row["id"])
            return

        db_operation_name, needs_select = EVENT_TYPE_MAPPINGS[event_type]
        db_operation = getattr(table, db_operation_name)()

        update_parent_op = update_parent_row = None
        if schema_table.has_parent_table and schema_table.parent_table_field.relation:
            # Have 1:n relation. We need to update the relation columns in the parent table as
            # well. Skips this for n:m relations (schematable.parent_table_field.relation only
            # returns 1:n relations)
            dataset_id = event_meta["dataset_id"]
            rel_field_prefix = to_snake_case(schema_table.parent_table_field.shortname)
            parent_schema_table = schema_table.parent_table
            parent_table = self.tables[dataset_id][parent_schema_table.id]
            parent_id_field = (
                parent_table.c.id
                if parent_schema_table.has_composite_key
                else getattr(parent_table.c, parent_schema_table.identifier[0])
            )
            parent_id_value = ".".join(
                [
                    str(row[to_snake_case(f"{parent_schema_table.id}_{fn}")])
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
            self.lasteventids.update_eventid(self.conn, schema_table, event_meta["event_id"])

            if update_parent_op is not None:
                self.conn.execute(update_parent_op, update_parent_row)

    def _row_exists_in_database(self, run_configuration: RunConfiguration, id_value: str):
        table = run_configuration.table
        schema_table = run_configuration.schema_table
        id_field = (
            table.c.id
            if schema_table.has_composite_key
            else getattr(table.c, schema_table.identifier[0])
        )

        with self.conn.begin():
            res = self.conn.execute(table.select().where(id_field == id_value))
            try:
                next(res)
            except StopIteration:
                return False
            return True

    def _table_empty(self, table: Table):
        with self.conn.begin():
            res = self.conn.execute(table.select())
            try:
                next(res)
            except StopIteration:
                return True
            return False

    def _get_run_configuration(
        self, first_event_meta: dict, last_event_meta: dict, recovery_mode: bool
    ) -> RunConfiguration:
        run_configuration = RunConfiguration()
        dataset_id = first_event_meta["dataset_id"]
        table_id = first_event_meta["table_id"]

        if first_event_meta.get("full_load_sequence", False):
            table, schema_table = self._get_full_load_tables(dataset_id, table_id)
        else:
            schema_table = self.datasets[dataset_id].get_table_by_id(table_id)
            table = self.tables[dataset_id][to_snake_case(table_id)]

        run_configuration.table = table
        run_configuration.schema_table = schema_table

        if recovery_mode:
            self._recover(run_configuration, first_event_meta, last_event_meta)
        return run_configuration

    def _recover(
        self, run_configuration: RunConfiguration, first_event_meta: dict, last_event_meta: dict
    ):
        # If a message is redelivered, we need to enter recovery mode. Redelivery means something
        # has gone wrong
        # somewhere. We need to get back to a consistent state.
        # The actions to take in recovery mode depend on the type of message:
        # 1. full_load_sequence = False: This was a regular update event. Can be of any type
        #     (ADD/MODIFY/DELETE). MODIFY
        #     and DELETE are idempotent, but ADD is not. We need to check if the data is already
        #     in the database before
        #     trying to add it again.
        # 2. full_load_sequence = True with first_of_sequence = True: This should not be a problem.
        #     The first_of_sequence
        #     causes the table to be truncated, so we can just continue as normal. No need to check
        #     for existence.
        # 3. full_load_sequence = True with first_of_sequence = False and last_of_sequence = False:
        #     We should check
        #     for existence before adding event data to the table. Because first_of_sequence and
        #     last_of_sequence are
        #     both False, there are no other possible side effects to consider.
        # 4. full_load_sequence = True with first_of_sequence = False and last_of_sequence = True:
        #    If the target table
        #    is empty, we know that after_process was executed and that this message was handled
        #    correctly the first
        #    time it got delivered (because first_of_sequence = False, there should already have
        #    been data in the
        #    table). In that case we can ignore everything in this message; skip processing the
        #    events and skip the
        #    after_process step (4a). If the target table is not empty, we know that after_process
        #    was not executed. We
        #    should process the events and check for existence of the first event. After that we
        #    should execute
        #    after_process (4b).
        if first_event_meta.get("full_load_sequence", False):
            if first_event_meta.get("first_of_sequence", False):
                # Case 2.
                pass
            elif not last_event_meta.get("last_of_sequence", False):
                # Case 3.
                run_configuration.check_existence_on_add = True
            else:
                # Case 4.
                if self._table_empty(run_configuration.table):
                    # Case 4a.
                    run_configuration.execute_after_process = False
                    run_configuration.process_events = False
                else:
                    # Case 4b.
                    run_configuration.check_existence_on_add = True
        else:
            # Case 1.
            run_configuration.check_existence_on_add = True

    def process_event(self, event_meta: dict, event_data: dict, recovery_mode: bool = False):
        self.process_events([(event_meta, event_data)], recovery_mode)

    def process_events(self, events: list[tuple[dict, dict]], recovery_mode: bool = False):
        if len(events) == 0:
            return

        first_event_meta, last_event_meta = events[0][0], events[-1][0]
        run_configuration = self._get_run_configuration(
            first_event_meta, last_event_meta, recovery_mode
        )

        self._before_process(run_configuration, first_event_meta)

        if run_configuration.process_events:
            if first_event_meta.get("full_load_sequence", False):
                # full_load_sequence only contains add events. Take more efficient shortcut.
                self._process_bulk_adds(run_configuration, events)
            else:
                for event_meta, event_data in events:
                    self._process_row(run_configuration, event_meta, event_data)

        if run_configuration.execute_after_process:
            self._after_process(run_configuration, last_event_meta)

    def _process_bulk_adds(
        self, run_configuration: RunConfiguration, events: list[tuple[dict, dict]]
    ):
        first = True
        rows = []
        for event_meta, event_data in events:
            if event_meta["event_type"] != "ADD":
                raise Exception("This method should only be called when processing ADD events.")

            row = self._prepare_row(event_meta, event_data, run_configuration.schema_table)
            if run_configuration.check_existence_on_add and first:
                if self._row_exists_in_database(run_configuration, row["id"]):
                    logger.info("Skip bulk adds, as the first row already exists in the database.")
                    return
                first = False
            rows.append(row)

        with self.conn.begin():
            self.conn.execute(run_configuration.table.insert(), rows)

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
