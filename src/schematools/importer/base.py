import hashlib
import operator
from collections import Counter, UserDict
from functools import reduce
from itertools import islice
from logging import Logger
from pathlib import PosixPath
from typing import Any, Dict, Iterator, List, Optional, Set, Union, cast

from jsonpath_rw import parse
from jsonpath_rw.jsonpath import Child
from sqlalchemy import exc, inspect
from sqlalchemy.dialects.postgresql.base import PGInspector
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Index, MetaData, Table

from schematools import DATABASE_SCHEMA_NAME_DEFAULT, MAX_TABLE_NAME_LENGTH, TABLE_INDEX_POSTFIX
from schematools.factories import tables_factory
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import to_snake_case, toCamelCase

metadata = MetaData()


def chunked(generator: Iterator[Any], size: int) -> Iterator[Any]:
    """Read parts of the generator, pause each time after a chunk."""
    # Based on more-itertools. islice returns results until 'size',
    # iter() repeatedly calls make_chunk until the '[]' sentinel is returned.
    gen = iter(generator)
    make_chunk = lambda: list(islice(gen, size))
    return iter(make_chunk, [])


class JsonPathException(Exception):
    """Exception used to signal a jsonpath provenance."""

    pass


class Row(UserDict):
    """Dict-based class that used provenance to find values."""

    # class-level cache for jsonpath expressions
    _expr_cache: Dict[str, Child] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializer that sets the provenance information."""
        fields_provenances = kwargs.pop("fields_provenances", {})
        self.rev_provenances: Dict[str, str] = {
            name: prov_name for prov_name, name in fields_provenances.items()
        }
        self.field_provenanced_by_id: Optional[str] = fields_provenances.get("id")
        # Provenanced keys are stored in a cache. This is not only for efficiency.
        # Sometimes, the key that is 'provenanced' is the same key that is in the ndjson
        # import data. When this key gets replaced, the original object structure
        # is not available anymore, so subsequent jsonpath lookups will fail.
        self.provenances_cache: Dict[str, str] = {}

        super().__init__(*args, **kwargs)

        if self.field_provenanced_by_id is not None:
            self.data[self.field_provenanced_by_id] = self.data["id"]

    def __getitem__(self, key: str) -> Any:
        """Gets a value taking provenance into account."""
        try:
            value = super().__getitem__(self._transform_key(key))
        except JsonPathException:
            value = self.provenances_cache.get(key) or self._fetch_value_for_jsonpath(key)
            self.provenances_cache[key] = value
        return value

    def __delitem__(self, key: str) -> None:
        """Deletes a value taking provenance into account."""
        try:
            return super().__delitem__(self._transform_key(key))
        except JsonPathException:
            # For JsonPath keys, a __del__ should use the
            # key that is originally provided
            return super().__delitem__(key)

    def _transform_key(self, key: str) -> str:
        if key == self.field_provenanced_by_id:
            return self.field_provenanced_by_id
        prov_key = self.rev_provenances.get(key)
        if prov_key is not None:
            if prov_key.startswith("$"):
                raise JsonPathException()
            return prov_key
        if key in self.data:
            return key
        raise KeyError

    def _fetch_expr(self, prov_key: str) -> Child:
        if prov_key in self._expr_cache:
            return self._expr_cache[prov_key]
        expr = parse(prov_key)
        self._expr_cache[prov_key] = expr
        return expr

    def _fetch_value_for_jsonpath(self, key: str) -> Optional[Union[int, str]]:
        prov_key = self.rev_provenances.get(key)
        if prov_key is None:
            return None
        top_element_name = prov_key.split(".")[1]
        expr = self._fetch_expr(prov_key)
        # Sometimes the data is not an object, but simply None
        top_level_data = self.data[top_element_name]
        if top_level_data is None:
            self.data[key] = None
            return None
        matches = expr.find({top_element_name: top_level_data})
        value = None if not matches else matches[0].value
        self.data[key] = value
        return value


class BaseImporter:
    """Base importer that holds common data."""

    def __init__(
        self, dataset_schema: DatasetSchema, engine: Engine, logger: Optional[Logger] = None
    ) -> None:
        """Initializes the BaseImporter.

        dataset_schema: The dataset to work with.
        engine: SQLAlchemy database engine.
        """
        self.engine = engine
        self.dataset_schema = dataset_schema
        self.srid = dataset_schema["crs"].split(":")[-1]
        self.dataset_table: Optional[DatasetTableSchema] = None
        self.fields_provenances: Dict[str, str] = {}
        self.db_table_name: Optional[str] = None
        self.tables: Dict[str, Table] = {}
        self.indexes: Dict[str, List[Index]] = {}
        self.pk_values_lookup: Dict[str, Set[Any]] = {}
        self.pk_colname_lookup: Dict[str, str] = {}
        self.logger = LogfileLogger(logger) if logger else CliLogger()

    def fetch_fields_provenances(self, dataset_table: DatasetTableSchema) -> Dict[str, str]:
        """Create mapping from provenance to camelcased fieldname."""
        fields_provenances = {}
        for field in dataset_table.fields:
            if (provenance := field.get("provenance")) is not None:
                fields_provenances[provenance] = field.name
        return fields_provenances

    def fix_fieldnames(
        self, fields_provenances: Dict[str, str], table_records: Iterator[Any]
    ) -> Any:
        """Fixes the fieldname.

        We need relational snakecased fieldnames in the records and,
        we need to take provenance in the input records into account.
        """
        fixed_records = []
        for record in table_records:
            fixed_record = {}
            for field_name, field_value in record.items():
                if field_name == "id":
                    fixed_record["id"] = field_value
                    continue
                fixed_field_name = fields_provenances.get(field_name, field_name)
                fixed_record[to_snake_case(fixed_field_name)] = field_value
            fixed_records.append(fixed_record)
        return fixed_records

    def deduplicate(
        self,
        table_name: str,
        table_records: List[Row],
    ) -> Iterator[Row]:
        """Removes duplicates from a set of records."""
        this_batch_pk_values = set()
        pk_name = self.pk_colname_lookup.get(table_name)

        values_lookup: Set[Any] = self.pk_values_lookup.get(table_name, set())

        for record in table_records:
            if pk_name is None:
                yield record
                continue
            value = record[toCamelCase(pk_name)]
            if value not in values_lookup and value not in this_batch_pk_values:
                yield record
            else:
                self.logger.log_warning(
                    "Duplicate record for %s, with %s = %s", table_name, pk_name, value
                )
            this_batch_pk_values.add(value)
            values_lookup.add(value)

    def create_pk_lookup(self, tables: Dict[str, Table]) -> None:
        """Generate a lookup to avoid primary_key clashes."""
        for table_name, table in tables.items():
            if isinstance(table, Table):
                pk_columns = inspect(table).primary_key.columns
                # nm tables do not have a PK
                if not pk_columns:
                    continue
                # We assume a single PK (because of Django)
                pk_col = pk_columns.values()[0]
                pk_name = pk_col.name
                if pk_col.autoincrement:
                    continue
                self.pk_colname_lookup[table_name] = pk_name
                pks = {getattr(r, pk_name) for r in self.engine.execute(table.select())}

                self.pk_values_lookup[table_name] = pks

    def generate_db_objects(
        self,
        table_id: str,
        db_schema_name: Optional[str] = None,
        db_table_name: Optional[str] = None,
        truncate: bool = False,
        ind_tables: bool = True,
        ind_extra_index: bool = True,
        limit_tables_to: Optional[Set] = None,
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
        """
        self.dataset_table = cast(
            DatasetTableSchema, self.dataset_schema.get_table_by_id(table_id)
        )
        if ind_tables or ind_extra_index:

            # Collect provenance info for easy re-use
            self.fields_provenances = self.fetch_fields_provenances(self.dataset_table)
            self.db_table_name = db_table_name
            # Bind the metadata
            metadata.bind = self.engine
            # Get a table to import into

            if (dataset := self.dataset_table.dataset) is None:
                raise ValueError("Table {table_id} does not belong to a dataset")

            self.tables = tables_factory(
                dataset,
                metadata=metadata,
                db_schema_names={table_id: db_schema_name},
                db_table_names={table_id: db_table_name},
                limit_tables_to=limit_tables_to,
            )

        if db_table_name is None:
            self.db_table_name = self.dataset_table.db_name()

        if ind_tables:
            self.prepare_tables(self.tables, truncate=truncate)
            self.create_pk_lookup(self.tables)

        if ind_extra_index:
            try:
                # Get indexes to create
                self.indexes = index_factory(
                    self.dataset_table,
                    ind_extra_index,
                    db_schema_name=db_schema_name,
                    metadata=metadata,
                    db_table_name=self.db_table_name,
                )
                metadata_inspector = inspect(metadata.bind)
                self.prepare_extra_index(
                    self.indexes,
                    metadata_inspector,
                    metadata.bind,
                    db_schema_name,
                )
            except exc.NoInspectionAvailable:
                pass

    def load_file(
        self,
        file_name: PosixPath,
        batch_size: int = 100,
        is_through_table: bool = False,
        **kwargs: Any,
    ) -> Optional[Row]:
        """Import a file into the database table, returns the last record, if available."""
        if self.dataset_table is None:
            raise ValueError("Import needs to be initialized with table info")
        data_generator = self.parse_records(
            file_name,
            self.dataset_table,
            self.db_table_name,
            is_through_table=is_through_table,
            **{"fields_provenances": self.fields_provenances, **kwargs},
        )
        self.logger.log_start(file_name, size=batch_size)

        num_imported = 0
        insert_statements = {
            table_name: table.insert() for table_name, table in self.tables.items()
        }

        last_record: Optional[Row] = None
        for records in chunked(data_generator, size=batch_size):
            # every record is keyed on tablename + inside there is a list
            for table_name, insert_statement in insert_statements.items():
                table_records: List[Row] = reduce(
                    operator.add, [record.get(table_name, []) for record in records], []
                )
                table_records = self.fix_fieldnames(
                    self.fields_provenances,
                    self.deduplicate(table_name, table_records),
                )
                if table_records:
                    self.engine.execute(
                        insert_statement,
                        table_records,
                    )
            num_imported += len(records)
            self.logger.log_progress(num_imported)
            # main table is keyed on tablename and only has one row
            last_record = records[-1][self.dataset_table.name][0]

        self.logger.log_done(num_imported)
        return last_record

    def parse_records(
        self,
        filename: PosixPath,
        dataset_table: DatasetTableSchema,
        db_table_name: Optional[str] = None,
        is_through_table: bool = False,
        **kwargs: Any,
    ) -> Iterator[Dict[str, List[Row]]]:
        """Yield all records from the filename."""
        raise NotImplementedError()

    def prepare_tables(self, tables: Dict[str, Table], truncate: bool = False) -> None:
        """Create the tables if needed."""
        for table in tables.values():
            if isinstance(table, Table):
                if not table.exists():
                    table.create()
                elif truncate:
                    self.engine.execute(table.delete())

    def prepare_extra_index(
        self,
        indexes: Dict[str, List[Index]],
        inspector: PGInspector,
        engine: Engine,
        db_schema_name: Optional[str] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        """Create extra indexes.

        Indexs are placed on identifiers columns in base tables
        and identifier columns in n:m tables, if not exists.
        """
        # setup logger
        _logger: CliLogger = LogfileLogger(logger) if logger else CliLogger()

        # In the indexes dict, the table name of each index, is stored in the key
        target_table_names = list(indexes.keys())
        missing_table_names = set()

        # get current DB indexes on table
        current_db_indexes = set()
        for table in target_table_names:
            try:
                db_indexes = inspector.get_indexes(table, schema=db_schema_name)
            except exc.NoSuchTableError:
                missing_table_names.add(table)
                continue
            for current_db_index in db_indexes:
                current_db_indexes.add(current_db_index["name"])

        # get all found indexes generated out of Amsterdam schema
        schema_indexes = set()
        schema_indexes_objects = {}
        for index_object_list in indexes.values():
            for index_object in index_object_list:
                schema_indexes.add(index_object.name)
                schema_indexes_objects[index_object.name] = index_object

        # get difference between DB indexes en indexes found Amsterdam schema
        indexes_to_create = list(
            (Counter(schema_indexes) - Counter(current_db_indexes)).elements()
        )

        # create indexes - that do not exists yet- in DB
        for index_name in indexes_to_create:
            try:
                index = schema_indexes_objects[index_name]
                if index.table.name not in missing_table_names:
                    _logger.log_warning(f"Index '{index_name}' not found...creating")
                    index.create(bind=engine)

            except AttributeError as e:
                _logger.log_error(
                    f"Error creating index '{index_name}' for '{target_table_names}', error: {e}"
                )
                continue


class CliLogger:
    """Logger to be used when importer is called from the cli."""

    def log_start(self, file_name: PosixPath, size: int) -> None:
        """Start the logging."""
        print(f"Importing data [each dot is {size} records]: ", end="", flush=True)  # noqa: T001

    def log_progress(self, num_imported: int) -> None:
        """Output progress."""
        print(".", end="", flush=True)  # noqa: T001

    def log_error(self, msg: str, *args: Any) -> None:
        """Output error."""
        print(msg % args)  # noqa: T001

    def log_warning(self, msg: str, *args: Any) -> None:
        """Output warning."""
        print(msg % args)  # noqa: T001

    def log_done(self, num_imported: int) -> None:
        """Indicate logging is finished."""
        print(f" Done importing {num_imported} records", flush=True)  # noqa: T001


class LogfileLogger(CliLogger):
    """Logger to be used when importer is called from python code."""

    def __init__(self, logger: Logger):
        """Initialize logger."""
        self.logger = logger

    def log_start(self, file_name: PosixPath, size: int) -> None:
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


def index_factory(
    dataset_table: DatasetTableSchema,
    ind_extra_index: bool,
    metadata: Optional[MetaData] = None,
    db_table_name: Optional[str] = None,
    db_schema_name: Optional[str] = None,
    logger: Optional[Logger] = None,
) -> Dict[str, List[Index]]:
    """Generates one or more SQLAlchemy Index objects to work with the JSON Schema.

    :param dataset_table: The Amsterdam Schema definition of the table
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
    :param db_table_name: Optional table name, which is otherwise inferred from the schema name.
    :param db_schema_name: Optional database schema name, which is otherwise None
        and defaults to public.

    Identifier index:
    In the JSON Schema definition of the table, an identifier arry may be definied.
    I.e. "identifier": ["identificatie", "volgnummer"]
    This is frequently used in temporal tables where versions of data are present (history).

    Through table index:
    In the JSON schema definition of the table, relations may be definied.
    In case of temporal data, this will lead to intersection tables a.k.a. through tables to
    accomodate n:m relations.

    FK index:
    In the JSON schema definition of the table, relations may be 1:N relations definied.
    Where the child table is referencing a parent table.
    These reference are not enforced by a database foreign key constraint. But will be
    used in joins to collect data when calling a API endpoint. Therefore must be indexed
    for optimal performance.

    The returned Index objects are keyed on the name of table
    """
    indexes = {}
    _metadata = cast(MetaData, metadata or MetaData())
    _logger = LogfileLogger(logger) if logger else CliLogger()

    # This is really ugly, mypy forces a new local variable here,
    # otherwise it keeps complaining about the type of db_table_name being Optional.
    if db_table_name is None:
        _db_table_name = dataset_table.db_name()
    else:
        _db_table_name = db_table_name

    final_db_schema_name = (
        DATABASE_SCHEMA_NAME_DEFAULT if db_schema_name is None else db_schema_name
    )
    table_name = f"{final_db_schema_name}.{_db_table_name}"

    try:
        table_object = _metadata.tables[table_name]
    except KeyError as ex:
        _logger.log_error(f"{table_name} cannot be found.")

    indexes_to_create = []

    def make_hash_value(index_name: str) -> str:
        """Create a hash value for index_name.

        Postgres DB holds currently 63 max characters for objectnames.
        To prevent exceeds and collisions, the index names are shortend
        based upon a hash.
        With the blake2s algorithm a digest size is set to 20 bytes,
        which proceduces a 40 character long heximal string plus
        the additional 4 character postfix of '_idx' (TABLE_INDEX_POSTFIX).
        """
        return (
            hashlib.blake2s(index_name.encode(), digest_size=20).hexdigest() + TABLE_INDEX_POSTFIX
        )

    def define_fk_index() -> None:
        """Creates an index on Foreign Keys."""
        if dataset_table.get_fk_fields():

            for field in dataset_table.get_fk_fields():
                field_name = f"{to_snake_case(field)}_id"
                index_name = f"{_db_table_name}_{field_name}_idx"
                if len(index_name) > MAX_TABLE_NAME_LENGTH:
                    index_name = make_hash_value(index_name)
                indexes_to_create.append(Index(index_name, table_object.c[field_name]))

        # add Index objects to create
        indexes[_db_table_name] = indexes_to_create

    def define_identifier_index() -> None:
        """Creates index based on the 'identifier' specification in the Amsterdam schema."""
        identifier_column_snaked = []

        if not indexes_to_create and dataset_table.identifier:

            for identifier_column in dataset_table.identifier:
                try:
                    identifier_column_snaked.append(
                        # precautionary measure: If camelCase, translate to
                        # snake_case, so column(s) can be found in table.
                        table_object.c[to_snake_case(identifier_column)]
                    )
                except KeyError as e:
                    _logger.log_error(f"{e.__str__} on {dataset_table.id}.{identifier_column}")
                    continue

        index_name = f"{_db_table_name}_identifier_idx"
        if len(index_name) > MAX_TABLE_NAME_LENGTH:
            index_name = make_hash_value(index_name)
        indexes_to_create.append(Index(index_name, *identifier_column_snaked))

        # add Index objects to create
        indexes[_db_table_name] = indexes_to_create

    def define_throughtable_index() -> None:
        """Creates index(es) on the many-to-many tables.

        Those are based on 'relation' specification in the Amsterdam schema.
        """
        for table in dataset_table.get_through_tables_by_id():

            through_columns: List[str] = []
            indexes_to_create: List[Index] = []

            # make a dictionary of the indexes to create
            if table.is_through_table:
                through_columns = []

                # First collect the fields that are relations
                relation_field_names = []
                for field in table.fields:
                    if field.relation:
                        snakecased_fieldname = to_snake_case(field.name)
                        relation_field_names.append(snakecased_fieldname)
                        through_columns.append(f"{snakecased_fieldname}_id")

            # create the Index objects
            if through_columns:
                table_id = f"{final_db_schema_name}.{table.db_name()}"
                try:
                    table_object = _metadata.tables[table_id]

                except KeyError:
                    _logger.log_error(
                        f"Unable to create Indexes {table_id}. Table not found in DB."
                    )
                    continue

                for column in through_columns:
                    index_name = table_id + "_" + column + TABLE_INDEX_POSTFIX
                    if len(index_name) > MAX_TABLE_NAME_LENGTH:
                        index_name = make_hash_value(index_name)
                    try:
                        indexes_to_create.append(Index(index_name, table_object.c[column]))
                    except KeyError as e:
                        _logger.log_error(
                            f"{e.__str__}:{table_id}.{column} not found in {table_object.c}"
                        )
                        continue

                # add Index objects to create
                indexes[table.db_name()] = indexes_to_create

    if ind_extra_index:
        define_identifier_index()
        define_throughtable_index()
        define_fk_index()

    return indexes
