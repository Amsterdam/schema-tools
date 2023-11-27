"""Module implementing an event processor, that processes full events."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass

import orjson
from sqlalchemy import Table, inspect
from sqlalchemy.engine import Connection

from schematools.events import metadata
from schematools.exceptions import DatasetTableNotFound
from schematools.factories import tables_factory
from schematools.importer.base import BaseImporter
from schematools.loaders import get_schema_loader
from schematools.naming import to_snake_case
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema

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
class UpdateParentTableConfiguration:
    parent_schema_table: DatasetTableSchema
    parent_table: Table
    relation_name: str

    @property
    def parent_id_field(self):
        return (
            self.parent_table.c.id
            if self.parent_schema_table.has_composite_key
            else getattr(self.parent_table.c, self.parent_schema_table.identifier[0])
        )

    def parent_id_value(self, prepared_row: dict) -> str:
        return ".".join(
            [
                str(prepared_row[to_snake_case(f"{self.parent_schema_table.shortname}_{fn}")])
                for fn in self.parent_schema_table.identifier
            ]
        )

    @property
    def parent_fields(self) -> list[str]:
        pattern = re.compile(f"{self.relation_name}_[a-zA-Z]+")
        return [f.name for f in self.parent_table.columns if re.match(pattern, f.name) is not None]

    def parent_field_values(self, data: dict) -> dict:
        return {k: data[k] for k in self.parent_fields}


@dataclass
class RunConfiguration:
    check_existence_on_add: bool = False
    process_events: bool = True
    execute_after_process: bool = True

    """Whether or not to update the table. This should usually be the case, but this variable can
    be set to False when the table does not exist. This can happen for certain relation events
    for which no relation table is defined in the database. In this case we still want to update
    the relation columns in the parent table.

    """
    update_table: bool = True

    table: Table = None
    table_name: str = None
    schema_table: DatasetTableSchema = None

    update_parent_table_configuration: UpdateParentTableConfiguration = None

    nested_table_fields: list[DatasetFieldSchema] = None


class LastEventIds:
    BENK_DATASET = "benk"
    LASTEVENTIDS_TABLE = "lasteventids"

    def __init__(self):
        loader = get_schema_loader()
        self.dataset = loader.get_dataset(self.BENK_DATASET)
        self.lasteventids_table = self.dataset.get_table_by_id(self.LASTEVENTIDS_TABLE)
        self.lasteventid_column = self.lasteventids_table.get_field_by_id("lastEventId")

        self.cache = defaultdict(int)

    def is_event_processed(self, conn, table_name: str, event_id: int) -> bool:
        last_eventid = self.get_last_eventid(conn, table_name)

        return False if last_eventid is None else last_eventid >= event_id

    def update_eventid(self, conn, table_name: str, event_id: int | None):
        value = event_id if event_id is not None else "NULL"
        conn.execute(
            f"INSERT INTO {self.lasteventids_table.db_name} "  # noqa: S608  # nosec: B608
            f"VALUES ('{table_name}', {value}) "
            f'ON CONFLICT ("table") DO UPDATE SET {self.lasteventid_column.db_name} = {value}'
        )
        self.cache[table_name] = event_id

    def get_last_eventid(self, conn, table_name: str) -> int | None:
        if table_name in self.cache:
            return self.cache[table_name]

        res = conn.execute(
            f"SELECT {self.lasteventid_column.db_name} "  # noqa: S608
            f"FROM {self.lasteventids_table.db_name} "  # noqa: S608
            f"WHERE \"table\" = '{table_name}'"  # noqa: S608
        ).fetchone()
        last_eventid = None if res is None else res[0]
        self.cache[table_name] = last_eventid
        return last_eventid

    def copy_lasteventid(self, conn, from_table_name: str, to_table_name: str):
        eventid = self.get_last_eventid(conn, from_table_name)
        self.update_eventid(conn, to_table_name, eventid)

    def clear_cache(self):
        self.cache = defaultdict(int)


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
        benk_dataset = self.lasteventids.lasteventids_table.dataset
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
            # Initialise fresh tables for full load.
            dataset_schema: DatasetSchema = self.datasets[dataset_id]
            tables = dataset_schema.get_tables(include_nested=True, include_through=True)

            importer = BaseImporter(dataset_schema, self.conn.engine, logger)
            for schema_table in tables:
                this_table_id = schema_table.id

                if to_snake_case(this_table_id) in self.full_load_tables[dataset_id]:
                    continue

                db_table_name = schema_table.db_name_variant(postfix=FULL_LOAD_TABLE_POSTFIX)

                importer.generate_db_objects(
                    this_table_id,
                    db_schema_name=to_snake_case(dataset_id),
                    db_table_name=db_table_name,
                    is_versioned_dataset=importer.is_versioned_dataset,
                    ind_extra_index=False,
                    ind_create_pk_lookup=False,
                )

                table = importer.tables[this_table_id]
                self.full_load_tables[dataset_id][to_snake_case(this_table_id)] = (
                    table,
                    schema_table,
                )
            try:
                table, schema_table = self.full_load_tables[dataset_id][table_id]
            except KeyError:
                # Happens when there is no relation table to update for a given relation, for ex.
                raise DatasetTableNotFound()

        return table, schema_table

    def _before_process(self, run_configuration: RunConfiguration, event_meta: dict) -> None:
        if not run_configuration.update_table:
            return
        if event_meta.get("full_load_sequence", False):

            if event_meta.get("first_of_sequence", False):
                self.conn.execute(f"TRUNCATE {run_configuration.table.fullname}")
                self.lasteventids.update_eventid(self.conn, run_configuration.table_name, None)

    def _after_process(self, run_configuration: RunConfiguration, event_meta: dict):
        if not run_configuration.update_table:
            return

        if event_meta.get("full_load_sequence", False) and event_meta.get(
            "last_of_sequence", False
        ):
            dataset_id = event_meta["dataset_id"]
            table_id = event_meta["table_id"]

            nested_tables = [
                to_snake_case(f.nested_table.id) for f in run_configuration.nested_table_fields
            ]
            table_ids_to_replace = [
                table_id,
            ] + nested_tables

            logger.info("End of full load sequence. Replacing active table.")
            with self.conn.begin():

                full_load_tables = []
                for t_id in table_ids_to_replace:
                    table_to_replace = self.tables[dataset_id][to_snake_case(t_id)]
                    full_load_table, full_load_schema_table = self._get_full_load_tables(
                        dataset_id, to_snake_case(t_id)
                    )
                    full_load_tables.append((full_load_table, full_load_schema_table))

                    fields = [field.db_name for field in full_load_schema_table.get_db_fields()]
                    if t_id in nested_tables:
                        fields.remove("id")  # Let PG generate the id field for nested tables.
                    fieldnames = ", ".join(fields)

                    self.conn.execute(f"TRUNCATE {table_to_replace.fullname}")
                    self.conn.execute(
                        f"INSERT INTO {table_to_replace.fullname} ({fieldnames}) "  # noqa: S608
                        f"SELECT {fieldnames} FROM {full_load_table.fullname}"  # noqa: S608
                    )
                if run_configuration.update_parent_table_configuration:
                    self._update_parent_table_bulk(run_configuration)

                for full_load_table, full_load_schema_table in full_load_tables:
                    self.conn.execute(f"DROP TABLE {full_load_table.fullname} CASCADE")
                    self.full_load_tables[dataset_id].pop(to_snake_case(full_load_schema_table.id))

                # Copy full_load lasteventid to active table and set full_load lasteventid to None
                self.lasteventids.copy_lasteventid(
                    self.conn, run_configuration.table_name, table_to_replace.name
                )
                self.lasteventids.update_eventid(self.conn, run_configuration.table_name, None)

    def _prepare_row(
        self,
        run_configuration: RunConfiguration,
        event_meta: dict,
        event_data: dict,
        schema_table: DatasetTableSchema,
    ) -> dict:
        dataset_id = event_meta["dataset_id"]
        table_id = event_meta["table_id"]

        row = self._flatten_event_data(event_data)

        # Set null values for missing fields after flattening (e.g. when a 1-1 relation is empty)
        # Only applies to events for which we have a table
        if schema_table:
            row |= {
                f.db_name: None
                for f in schema_table.get_fields(include_subfields=True)
                if f.db_name not in row and f.db_name not in ("id", "schema")
            }

        for geo_field in self.geo_fields[dataset_id][table_id]:
            row_key = to_snake_case(geo_field.name)
            geo_value = row.get(row_key)
            if geo_value is not None and not geo_value.startswith("SRID"):
                row[row_key] = f"SRID={geo_field.srid};{geo_value}"

        if run_configuration.update_table:
            identifier = schema_table.identifier
            id_value = ".".join(str(row[to_snake_case(fn)]) for fn in identifier)
            row["id"] = id_value
        return row

    def _update_parent_table(
        self,
        configuration: UpdateParentTableConfiguration,
        event_meta: dict,
        prepared_row: dict,
    ):
        # Have 1:n relation. We need to update the relation columns in the parent table as
        # well. Skips this for n:m relations (schematable.parent_table_field.relation only
        # returns 1:n relations)
        stmt = configuration.parent_table.update().where(
            configuration.parent_id_field == configuration.parent_id_value(prepared_row)
        )

        update_row = (
            configuration.parent_field_values(prepared_row)
            if event_meta["event_type"] != "DELETE"
            else {k: None for k in configuration.parent_fields}
        )

        self.conn.execute(stmt, update_row)

    def _update_parent_table_bulk(self, run_configuration: RunConfiguration):
        update_parent_table_config = run_configuration.update_parent_table_configuration
        parent_table_ref_id = f"{run_configuration.schema_table.shortname.split('_')[0]}_id"

        if len(update_parent_table_config.parent_fields) > 1:
            # Adds parentheses
            set_fields = (
                f'({", ".join(update_parent_table_config.parent_fields)}) = '
                f'(s.{", s.".join(update_parent_table_config.parent_fields)})'
            )
        else:
            # No parentheses, because only one field and Postgres will fail on this
            set_fields = (
                f"{update_parent_table_config.parent_fields[0]} = "
                f"s.{update_parent_table_config.parent_fields[0]}"
            )

        query = f"""
        UPDATE {update_parent_table_config.parent_table.fullname} p
        SET {set_fields}
        FROM {run_configuration.table.fullname} s
        WHERE p.{update_parent_table_config.parent_id_field.name} = s.{parent_table_ref_id}
        ;
        """  # noqa: S608

        self.conn.execute(query)

    def _process_row(
        self, run_configuration: RunConfiguration, event_meta: dict, event_data: dict
    ) -> None:
        """Process one row of data.

        Args:
            run_configuration: Configuration for the current run
            event_meta: Metadata about the event
            event_data: Data containing the fields of the event
        """
        table = run_configuration.table
        schema_table = run_configuration.schema_table

        if self.lasteventids.is_event_processed(
            self.conn, run_configuration.table_name, event_meta["event_id"]
        ):
            logger.warning("Event with id %s already processed. Skipping.", event_meta["event_id"])
            return

        row = self._prepare_row(run_configuration, event_meta, event_data, schema_table)
        id_value = row["id"]

        event_type = event_meta["event_type"]

        db_operation = None
        if run_configuration.update_table:
            if (
                run_configuration.check_existence_on_add
                and event_type == "ADD"
                and self._row_exists_in_database(run_configuration, id_value)
            ):
                logger.info("Row with id %s already exists in database. Skipping.", row["id"])
                return

            db_operation_name, needs_select = EVENT_TYPE_MAPPINGS[event_type]
            db_operation = getattr(table, db_operation_name)()

            if needs_select:
                id_field = (
                    table.c.id
                    if schema_table.has_composite_key
                    else getattr(table.c, schema_table.identifier[0])
                )
                db_operation = db_operation.where(id_field == id_value)
        with self.conn.begin():
            if run_configuration.update_table:
                self.conn.execute(db_operation, row)
                self._update_nested_tables(run_configuration, row, event_type, id_value)
            self.lasteventids.update_eventid(
                self.conn, run_configuration.table_name, event_meta["event_id"]
            )

            if run_configuration.update_parent_table_configuration:
                self._update_parent_table(
                    run_configuration.update_parent_table_configuration,
                    event_meta,
                    row,
                )

    def _update_nested_tables(
        self, run_configuration: RunConfiguration, prepared_row: dict, event_type: str, id_value
    ):
        is_delete = event_type == "DELETE"

        for field in run_configuration.nested_table_fields:
            schema_table: DatasetTableSchema = field.nested_table
            table = self.tables[run_configuration.schema_table.dataset.id][
                to_snake_case(schema_table.id)
            ]

            self.conn.execute(table.delete().where(table.c.parent_id == id_value))

            if is_delete:
                continue

            if value := prepared_row.get(to_snake_case(field.id), []):
                if rows := self._prepare_nested_rows(field, value, id_value):
                    self.conn.execute(table.insert(), rows)

    def _prepare_nested_rows(self, field: DatasetFieldSchema, value: list, parent_id_value: str):
        return [
            {
                "parent_id": parent_id_value,
            }
            | {subfield.db_name: v[subfield.db_name] for subfield in field.subfields}
            for v in value
        ]

    def _update_nested_tables_bulk(self, run_configuration: RunConfiguration, rows: list[dict]):

        for field in run_configuration.nested_table_fields:
            schema_table: DatasetTableSchema = field.nested_table
            table, _ = self._get_full_load_tables(
                run_configuration.schema_table.dataset.id, to_snake_case(schema_table.id)
            )

            nested_rows = []
            for row in rows:
                nested_rows += self._prepare_nested_rows(
                    field, row.get(to_snake_case(field.id)) or [], row["id"]
                )
            if nested_rows:
                self.conn.execute(table.insert(), nested_rows)

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

        try:
            if first_event_meta.get("full_load_sequence", False):
                table, schema_table = self._get_full_load_tables(
                    dataset_id, to_snake_case(table_id)
                )
            else:
                schema_table = self.datasets[dataset_id].get_table_by_id(table_id)
                table = self.tables[dataset_id][to_snake_case(table_id)]
            run_configuration.table = table
            run_configuration.schema_table = schema_table
            run_configuration.table_name = table.name
            run_configuration.nested_table_fields = [
                f for f in schema_table.fields if f.is_nested_table
            ]

            if schema_table.has_parent_table and schema_table.parent_table_field.relation:
                run_configuration.update_parent_table_configuration = (
                    UpdateParentTableConfiguration(
                        parent_schema_table=schema_table.parent_table,
                        parent_table=self.tables[dataset_id][schema_table.parent_table.id],
                        relation_name=to_snake_case(schema_table.parent_table_field.shortname),
                    )
                )
        except DatasetTableNotFound as exc:
            # Check if relation table, if so continue
            parent_table_id, *field_id = table_id.split("_")
            field_id = "_".join(field_id)

            parent_table = self.datasets[dataset_id].get_table_by_id(parent_table_id)
            field = parent_table.get_field_by_id(field_id)
            if field.get("relation"):
                logger.info(
                    "Relation %s.%s has no table. Will only update parent table.",
                    dataset_id,
                    table_id,
                )
                run_configuration.update_table = False
                run_configuration.update_parent_table_configuration = (
                    UpdateParentTableConfiguration(
                        parent_schema_table=parent_table,
                        parent_table=self.tables[dataset_id][parent_table_id],
                        relation_name=to_snake_case(field.shortname),
                    )
                )
                run_configuration.table_name = to_snake_case(table_id)

            else:
                raise exc

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
        last_eventid = None

        for event_meta, event_data in events:
            if event_meta["event_type"] != "ADD":
                raise Exception("This method should only be called when processing ADD events.")

            row = self._prepare_row(
                run_configuration, event_meta, event_data, run_configuration.schema_table
            )
            if (
                run_configuration.check_existence_on_add
                and first
                and run_configuration.update_table
            ):
                if self._row_exists_in_database(run_configuration, row["id"]):
                    logger.info("Skip bulk adds, as the first row already exists in the database.")
                    return
                first = False

            # Note: This has some overlap with the recovery mode
            if self.lasteventids.is_event_processed(
                self.conn, run_configuration.table_name, event_meta["event_id"]
            ):
                logger.warning(
                    "Skip event with id %s, as the event has already been processed.",
                    event_meta["event_id"],
                )
                continue

            rows.append(row)
            last_eventid = event_meta["event_id"]

        if len(rows) == 0:
            return

        with self.conn.begin():
            if run_configuration.update_table:
                self.conn.execute(run_configuration.table.insert(), rows)
                self._update_nested_tables_bulk(run_configuration, rows)
            self.lasteventids.update_eventid(self.conn, run_configuration.table_name, last_eventid)

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
