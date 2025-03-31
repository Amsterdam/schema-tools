from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from contextlib import closing
from functools import cached_property
from itertools import islice
from pathlib import Path
from typing import Any, Final, TypeVar

import click
import jsonpath_rw
from psycopg import sql
from psycopg.errors import DuplicateSchema
from sqlalchemy import Boolean, exc, inspect, select, text
from sqlalchemy.dialects.postgresql.base import PGInspector
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.ddl import CreateSchema
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import Index, MetaData, Table

from schematools.factories import index_factory, tables_factory, views_factory
from schematools.types import DatasetSchema, DatasetTableSchema

metadata = MetaData()

#: We'd like to make a distinction between existing datasets that were originally, and
#: continue to be, created in the ``public`` PostgreSQL schema and newer datasets that will get
#: their own PostgreSQL schema. These newer datasets will also be versioned. Meaning that all
#: tables of these newer datasets will include their major and minor version in their name. Hence
#: the name of the SQL query to detect these newer datasets
IS_VERSIONED_DATASET_SQL: Final[TextClause] = text(
    """
    WITH public_dataset AS (  -- 'public' as in using public psql schema for a given dataset
        SELECT DISTINCT SPLIT_PART(TABLE_NAME, '_', 1) AS dataset_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'  -- specifically not 'VIEW'
            ORDER BY dataset_name),
         private_dataset AS (  -- 'private' as in using dataset specific psql schema
             SELECT schema_name AS dataset_name
                 FROM information_schema.schemata
                 WHERE schema_name NOT IN ('pg_toast',
                                           'pg_catalog',
                                           'public',
                                           'information_schema'
                     ))
    SELECT :dataset_name NOT IN (SELECT * FROM public_dataset) OR
           :dataset_name IN (SELECT * FROM private_dataset) AS is_versioned_dataset;
"""
).columns(is_versioned_dataset=Boolean)

T = TypeVar("T")


def chunked(stream: Iterator[T], size: int) -> Iterator[list[T]]:
    """Read parts of the generator, pause each time after a chunk."""

    # Based on more-itertools. islice returns results until 'size',
    # iter() repeatedly calls make_chunk until the '[]' sentinel is returned.
    def make_chunk():
        return list(islice(stream, size))

    return iter(make_chunk, [])


class JsonPathException(Exception):
    """Exception used to signal a jsonpath provenance."""


class Provenance:
    """Handler for the 'provenance' of a single field.
    This can resolve a field from an alias or JSONPath syntax.
    """

    def __init__(self, provenance: str):
        self.provenance: str = provenance
        self._json_path: jsonpath_rw.Child | None = (
            jsonpath_rw.parse(provenance) if provenance.startswith("$") else None
        )

    def __repr__(self):
        return f"Provenance({self.provenance!r})"

    def resolve(self, source: dict) -> Any:
        """Resolve provenance entries, return the value."""
        if self._json_path is not None:
            # JSONPath lookup
            matches = self._json_path.find(source)
            if not matches:
                raise LookupError(self.provenance)
            return matches[0].value
        else:
            # Alias name lookup
            return source[self.provenance]


class Record(dict):
    """A single generated row from the importer.
    Each dict key matches the database field name, each value is the data that should be written.

    This is a tiny wrapper around dict for simplicity that allows the source record
    to be accessed. The source record is useful debugging information,
    and some implementations need to retrieve non-schema fields
    (such as a "cursor" field in the last record).
    """

    def __init__(self, data, source: dict):
        super().__init__(data)
        self.source = source

    def __repr__(self):
        data_repr = super().__repr__()
        return f"Record({data_repr}, source={self.source!r})"


class BaseImporter:
    """Base importer that holds common data."""

    def __init__(
        self, dataset_schema: DatasetSchema, engine: Engine, logger: logging.Logger | None = None
    ) -> None:
        """Initializes the BaseImporter.

        dataset_schema: The dataset to work with.
        engine: SQLAlchemy database engine.
        """
        self.engine = engine
        self.dataset_schema = dataset_schema
        self.dataset_table: DatasetTableSchema | None = None
        self.db_table_name: str | None = None
        self.tables: dict[str, Table] = {}
        self.views: dict[str, sql.SQL] = {}
        self.pk_values_lookup: dict[str, set[Any]] = {}
        self.pk_colname_lookup: dict[str, str] = {}
        self.logger = LogfileLogger(logger) if logger else CliLogger()

    def deduplicate(
        self,
        table_name: str,
        table_records: list[Record],
    ) -> Iterator[Record]:
        """Removes duplicates from a set of records.
        This is introduced because the importer often needed to re-run
        before completing the task (e.g. source system errors / task redeploys).
        """
        pk_name = self.pk_colname_lookup.get(table_name)
        if pk_name is None:
            yield from table_records
            return

        # See which values already exist in the database. avoid reimporting them.
        values_lookup: set[Any] = self.pk_values_lookup.get(table_name, set())

        for record in table_records:
            value = record[pk_name]
            if value in values_lookup:
                self.logger.log_warning(
                    "Duplicate record for %s, with %s = %s", table_name, pk_name, value
                )
            else:
                yield record

            values_lookup.add(value)

    def create_pk_lookup(self, tables: dict[str, Table]) -> None:
        """Generate a lookup to avoid primary_key clashes."""
        for table_name, table in tables.items():
            pk_columns = inspect(table).primary_key.columns
            # nm tables do not have a PK
            if not pk_columns:
                continue

            # We assume a single PK (because of Django)
            pk_col = pk_columns.values()[0]
            pk_name = pk_col.name
            if pk_col.autoincrement:
                continue

            with self.engine.connect() as conn:
                pks = {r[0] for r in conn.execute(select(pk_col)).fetchall()}

            self.pk_colname_lookup[table_name] = pk_name
            self.pk_values_lookup[table_name] = pks

    def _schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists in the database."""
        with self.engine.connect() as conn:
            return bool(
                conn.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = :name)"),
                    {"name": schema_name},
                )
            )

    def generate_db_objects(
        self,
        table_id: str,
        db_schema_name: str | None = None,
        db_table_name: str | None = None,
        truncate: bool = False,
        ind_tables: bool = True,
        ind_extra_index: bool = True,
        limit_tables_to: set | None = None,
        is_versioned_dataset: bool = False,
        ind_create_pk_lookup: bool = True,
    ) -> None:
        """Generate the tablemodels, tables and indexes.

        As default both table and index creation are set to True.

        Args:
            table_id: Name of the table as defined in the JSON schema defintion.
            db_schema_name: Name of the database schema where table should be
                created/present. Defaults to None (== public).
            db_table_name: Name of the table as defined in the database.
                Defaults to None.
            truncate: Indication to truncate table. Defaults to False.
            ind_tables: Indication to create table. Defaults to True.
            ind_extra_index: Indication to create indexes. Defaults to True.
            limit_tables_to: Only process the indicated tables. Normally, SA tables
                are generated for the whole dataset where `table_id` belongs to.
                Sometimes, this is not needed/wanted.
            is_versioned_dataset: Indicate whether the tables should be created in a private DB
                schema with a version in their name. See also:
                :attr:`.BaseImporter.is_versioned_dataset`. The private
                schema name will be derived from the dataset ID, unless overridden by the
                ``db_schema_name`` parameter.
        """
        self.dataset_table = self.dataset_schema.get_table_by_id(table_id)
        table_id = self.dataset_table.id  # get real-cased ID.

        # check if the dataset is a view
        if self.dataset_table.is_view:
            self.logger.log_info("Dataset %s is a view, skipping table generation", table_id)
            return False

        if is_versioned_dataset:
            if db_schema_name is None:
                # private DB schema instead of `public`
                db_schema_name = self.dataset_schema.id
            else:
                self.logger.log_warning(
                    "Versioning is specified, though schema name is explicitly overridden. "
                    "Is this really want you want?"
                )
            if db_table_name is None:
                db_table_name = self.dataset_table.db_name_variant(
                    # No dataset prefix as the tables will be created in their own
                    # private schema.
                    with_dataset_prefix=False,
                    with_version=True,
                )
            else:
                self.logger.log_warning(
                    "Versioning is specified, though table name is explicitly overridden. "
                    "Is this really what you want?"
                )

        if db_schema_name is not None and not self._schema_exists(db_schema_name):
            self.create_schema(db_schema_name)

        if ind_tables or ind_extra_index:
            if (dataset := self.dataset_table.dataset) is None:
                raise ValueError("Table {table_id} does not belong to a dataset")

            # Bind the metadata
            metadata.bind = self.engine

            # Get a table to import into
            self.tables = tables_factory(
                dataset,
                metadata=metadata,
                db_schema_names={table_id: db_schema_name},
                db_table_names={table_id: db_table_name},
                limit_tables_to=limit_tables_to,
                is_versioned_dataset=is_versioned_dataset,
            )

            if is_versioned_dataset:
                self.views = views_factory(dataset, self.tables)

        if db_table_name is None:
            db_table_name = self.dataset_table.db_name

        if ind_tables:
            self.prepare_tables(self.tables, truncate=truncate)
            if ind_create_pk_lookup:
                self.create_pk_lookup(self.tables)
            self.prepare_views()

        if ind_extra_index:
            # Get indexes to create
            indexes = index_factory(
                self.dataset_table,
                db_schema_name=db_schema_name,
                metadata=metadata,
                db_table_name=db_table_name,
                is_versioned_dataset=is_versioned_dataset,
            )
            metadata_inspector = inspect(metadata.bind)
            self.prepare_extra_index(
                indexes,
                metadata_inspector,
                metadata.bind,
                db_schema_name,
            )
            return None
        return None

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        return table_name in self.engine.table_names()

    def create_view_user(self, table) -> None:
        """Create view users/owner in the database."""
        view_user = table.get_view_user()
        if view_user is not None:
            with closing(self.engine.raw_connection()) as conn, closing(conn.cursor()) as cur:
                cur.execute(text(f"CREATE USER {view_user}"))
                conn.commit()

    def generate_view(self, dataset: DatasetSchema) -> None:
        """Generate a view for a schema."""
        raise NotImplementedError

    def load_file(self, file_name: Path, batch_size: int = 100, **kwargs: Any) -> Record | None:
        """Import a file into the database table, returns the last record, if available."""
        if self.dataset_table is None:
            raise ValueError("Import needs to be initialized with table info")
        data_generator = self.parse_records(
            file_name,
            self.dataset_table,
            **kwargs,
        )
        self.logger.log_start(file_name, size=batch_size)

        num_imported = 0
        insert_statements = {table_id: table.insert() for table_id, table in self.tables.items()}
        skipped_tables = set()

        last_record: Record | None = None
        for records in chunked(data_generator, size=batch_size):
            # every record is keyed on tablename + inside there is a list
            for table_id, table_records in self._group_records(records).items():
                try:
                    insert_statement = insert_statements[table_id]
                except KeyError:
                    if table_id not in skipped_tables:
                        # Show proper table db_name instead of confusing users with the internal ID
                        # If the resolving fails, the generator isn't producing proper table IDs
                        self.logger.log_info(
                            "Table '%s' was excluded, skipping!",
                            self.dataset_schema.get_table_by_id(table_id).db_name,
                        )
                        skipped_tables.add(table_id)
                    continue

                table_records = list(self.deduplicate(table_id, table_records))
                if table_records:
                    with self.engine.begin() as conn:
                        conn.execute(insert_statement, table_records)
            num_imported += len(records)
            self.logger.log_progress(num_imported)

            # Track the last record that's inserted for the main table.
            last_record = records[-1][self.dataset_table.id][0]

        self.logger.log_done(num_imported)
        return last_record

    def _group_records(self, records: list[dict[str, list[Record]]]) -> dict[str, list[Record]]:
        """Combine the records for a single table into a single set"""
        groups = defaultdict(list)
        for record in records:
            for table, rows in record.items():
                groups[table].extend(rows)

        return dict(groups)

    def parse_records(
        self, filename: Path, dataset_table: DatasetTableSchema, **kwargs: Any
    ) -> Iterator[dict[str, list[Record]]]:
        """Yield all records from the filename.
        The expected format of each returned row is::

            {
                "tableId1": [{"db_field1": "value", ...}, ...],
                "tableId2": [...],
            }
        """
        raise NotImplementedError()

    def prepare_tables(self, tables: dict[str, Table], truncate: bool = False) -> None:
        """Create the tables if needed."""
        for table in tables.values():
            table.create(self.engine, checkfirst=True)
            if truncate:
                with self.engine.begin() as conn:
                    conn.execute(table.delete())  # DELETE FROM table.

    def prepare_views(self) -> None:
        """Create views, if any."""
        if not self.views:
            return

        # sql.SQL requires an actual DBAPI connectionØ
        with closing(self.engine.raw_connection()) as conn, closing(conn.cursor()) as cur:
            for view in self.views.values():
                cur.execute(view)
            conn.commit()

    def prepare_extra_index(
        self,
        indexes: dict[str, list[Index]],
        inspector: PGInspector,
        engine: Engine,
        db_schema_name: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Create extra indexes.

        Indexs are placed on identifiers columns in base tables
        and identifier columns in n:m tables, if not exists.
        """
        # setup logger
        _logger: CliLogger = LogfileLogger(logger) if logger else CliLogger()

        # create indexes, that do not exists yet
        for table_name, index_objects in indexes.items():
            try:
                db_indexes = inspector.get_indexes(table_name, schema=db_schema_name)
            except exc.NoSuchTableError:
                _logger.log_warning(f"Table '{table_name}' not found...skipping index creation")
                continue

            existing_db_indexes = {idx["name"] for idx in db_indexes}

            for index in index_objects:
                if index.name not in existing_db_indexes:
                    _logger.log_warning(f"Index '{index.name}' not found...creating")
                    index.create(bind=engine, checkfirst=True)

    @cached_property
    def is_versioned_dataset(self) -> bool:
        """Returns whether versioning will be employed for the current dataset.

        NB. Because the versioning has not been fully completed yet,
        this method always return False for now.


        Strictly speaking datasets are not directly (as in: on the dataset level) versioned
        anymore. Its tables, however, are! That is, in the Amsterdam Schema corresponding to the
        dataset. Whether we employ versioning on the DB level depends on whether we are dealing
        with:

            * an existing dataset that has been created in the ``public`` PostgreSQL schema
            * a dataset in a dataset specific PostgreSQL schema
            * a brand new dataset with no current DB representation.

        Versioning will be used for the latter two. Not for the first one."""
        return False

        # XXX Disabled until the decision has been made how to continue with versioning!

        # with self.engine.connect() as connection:
        #     is_versioned = cast(
        #         bool,
        #         connection.scalar(IS_VERSIONED_DATASET_SQL, dataset_name=self.dataset_schema.id),
        #     )
        # return is_versioned

    def create_schema(self, db_schema_name: str) -> None:
        """Create DB Schema.

        Is a no-op if schema already exists.
        """
        with self.engine.begin() as connection:
            try:
                connection.execute(CreateSchema(db_schema_name))
                self.logger.log_info("Created SQL schema %r", db_schema_name)
            except ProgrammingError as pe:
                if not isinstance(pe.orig, DuplicateSchema):
                    # `CreateSchema` does not use the 'IF NOT EXISTS` clause.
                    # Hence we get an error if the schema already exists.
                    raise


class CliLogger:
    """Logger to be used when importer is called from the cli."""

    def log_start(self, file_name: Path, size: int) -> None:
        """Start the logging."""
        click.echo(
            f"Importing data from {file_name} [each dot is {size} records]: ", nl=False
        )  # noqa: T001

    def log_progress(self, num_imported: int) -> None:
        """Output progress."""
        click.echo(".", nl=False)  # noqa: T001

    def log_error(self, msg: str, *args: Any) -> None:
        """Output error."""
        click.echo(msg % args, err=True)  # noqa: T001

    def log_warning(self, msg: str, *args: Any) -> None:
        """Output warning."""
        click.echo(msg % args)  # noqa: T001

    def log_done(self, num_imported: int) -> None:
        """Indicate logging is finished."""
        click.echo(f" Done importing {num_imported} records")  # noqa: T001

    def log_info(self, msg: str, *args: Any) -> None:
        """Output informational message."""
        click.echo(msg % args)


class LogfileLogger(CliLogger):
    """Logger to be used when importer is called from python code."""

    def __init__(self, logger: logging.Logger):
        """Initialize logger."""
        self.logger = logger

    def log_start(self, file_name: Path, size: int) -> None:
        """Start the logging."""
        self.logger.info("Importing %s with %d records each:", file_name, size)

    def log_progress(self, num_imported: int) -> None:
        """Output progress."""
        self.logger.info("- imported %d records", num_imported)

    def log_error(self, msg: str, *args: Any) -> None:
        """Output error."""
        self.logger.error(msg, *args)

    def log_warning(self, msg: str, *args: Any) -> None:
        """Output warning."""
        self.logger.warning(msg, *args)

    def log_done(self, num_imported: int) -> None:
        """Indicate logging is finished."""
        self.logger.info("Done")

    def log_info(self, msg: str, *args: Any) -> None:
        """Output informational message."""
        self.logger.info(msg, *args)
