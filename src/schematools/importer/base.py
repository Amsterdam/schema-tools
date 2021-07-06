import hashlib
import operator
from collections import Counter, UserDict
from functools import reduce
from itertools import islice
from typing import Dict, Optional

from jsonpath_rw import parse
from more_itertools import first
from sqlalchemy import Column, ForeignKey, Index, Integer, MetaData, String, Table, exc, inspect

from schematools import MAX_TABLE_NAME_LENGTH, TABLE_INDEX_POSTFIX
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import shorten_name, to_snake_case, toCamelCase

from . import fetch_col_type

metadata = MetaData()


def chunked(generator, size):
    """Read parts of the generator, pause each time after a chunk"""
    # Based on more-itertools. islice returns results until 'size',
    # iter() repeatedly calls make_chunk until the '[]' sentinel is returned.
    gen = iter(generator)
    make_chunk = lambda: list(islice(gen, size))
    return iter(make_chunk, [])


class JsonPathException(Exception):
    pass


class Row(UserDict):

    # class-level cache for jsonpath expressions
    _expr_cache = {}

    def __init__(self, *args, **kwargs):
        self.fields_provenances = {
            name: prov_name for prov_name, name in kwargs.pop("fields_provenances", {}).items()
        }
        # Provenanced keys are stored in a cache. This is not only for efficiency.
        # Sometimes, the key that is 'provenanced' is the same key that is in the ndjson
        # import data. When this key gets replaced, the original object structure
        # is not available anymore, so subsequent jsonpath lookups will fail.
        self.provenances_cache = {}

        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        try:
            value = super().__getitem__(self._transform_key(key))
        except JsonPathException:
            value = self.provenances_cache.get(key) or self._fetch_value_for_jsonpath(key)
            self.provenances_cache[key] = value
        return value

    def __delitem__(self, key):
        try:
            return super().__delitem__(self._transform_key(key))
        except JsonPathException:
            # For JsonPath keys, a __del__ should use the
            # key that is originally provided
            return super().__delitem__(key)

    def _transform_key(self, key):
        prov_key = self.fields_provenances.get(key)
        if prov_key is not None:
            if prov_key.startswith("$"):
                raise JsonPathException()
            return prov_key
        if key in self.data:
            return key

    def _fetch_expr(self, prov_key):
        if prov_key in self._expr_cache:
            return self._expr_cache[prov_key]
        expr = parse(prov_key)
        self._expr_cache[prov_key] = expr
        return expr

    def _fetch_value_for_jsonpath(self, key):
        prov_key = self.fields_provenances.get(key)
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

    def __init__(self, dataset_schema: DatasetSchema, engine, logger=None):
        self.engine = engine
        self.dataset_schema = dataset_schema
        self.srid = dataset_schema["crs"].split(":")[-1]
        self.dataset_table = None
        self.fields_provenances = None
        self.db_table_name = None
        self.tables = {}
        self.indexes = []
        self.pk_values_lookup = {}
        self.pk_colname_lookup = {}
        self.logger = LogfileLogger(logger) if logger else CliLogger()

    def fetch_fields_provenances(self, dataset_table):
        """Create mapping from provenance to camelcased fieldname"""
        fields_provenances = {}
        for field in dataset_table.fields:
            # XXX no walrus until we can go to python 3.8 (airflow needs 3.7)
            # if (provenance := field.get("provenance")) is not None:
            provenance = field.get("provenance")
            if provenance is not None:
                fields_provenances[provenance] = field.name
        return fields_provenances

    def fix_fieldnames(self, fields_provenances, table_records):
        """We need relational snakecased fieldnames in the records
        And, we need to take provenance in the input records into account
        """
        fixed_records = []
        for record in table_records:
            fixed_record = {}
            for field_name, field_value in record.items():
                fixed_field_name = fields_provenances.get(field_name, field_name)
                fixed_record[to_snake_case(fixed_field_name)] = field_value
            fixed_records.append(fixed_record)
        return fixed_records

    def deduplicate(self, table_name, table_records):
        this_batch_pk_values = set()
        pk_name = self.pk_colname_lookup.get(table_name)

        values_lookup = self.pk_values_lookup.get(table_name)
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

    def create_pk_lookup(self, tables):
        """Generate a lookup to avoid primary_key clashes"""
        for table_name, table in tables.items():
            if isinstance(table, Table):
                pk_columns = inspect(table).primary_key.columns
                # nm tables do not have a PK
                if not pk_columns:
                    return
                # We assume a single PK (because of Django)
                pk_col = pk_columns.values()[0]
                pk_name = pk_col.name
                if pk_col.autoincrement == "auto":
                    continue
                self.pk_colname_lookup[table_name] = pk_name
                pks = {getattr(r, pk_name) for r in self.engine.execute(table.select())}
                self.pk_values_lookup[table_name] = pks

    def generate_db_objects(
        self,
        table_name: str,
        db_schema_name: Optional[str] = None,
        db_table_name: Optional[str] = None,
        truncate: bool = False,
        ind_tables: bool = True,
        ind_extra_index: bool = True,
    ):
        """Generate the tablemodels and tables and / or index on identifier
        as specified in the JSON data schema. As default both table and index
        creation are set to True.

        Args:
            table_name: Name of the table as defined in the id in the JSON schema defintion.
            db_schema_name: Name of the database schema where table should be
                created/present. Defaults to None (== public).
            db_table_name: Name of the table as defined in the database.
                Defaults to None.
            truncate: Indication to truncate table. Defaults to False.
            ind_tables: Indication to create table. Defaults to True.
            ind_extra_index: Indication to create indexes. Defaults to True.
        """

        if ind_tables or ind_extra_index:
            self.dataset_table = self.dataset_schema.get_table_by_id(table_name)

            # Collect provenance info for easy re-use
            self.fields_provenances = self.fetch_fields_provenances(self.dataset_table)
            self.db_table_name = db_table_name
            # Bind the metadata
            metadata.bind = self.engine
            # Get a table to import into
            self.tables = table_factory(
                self.dataset_table,
                db_schema_name=db_schema_name,
                metadata=metadata,
                db_table_name=db_table_name,
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
                    logger=[],
                )
                metadata_inspector = inspect(metadata.bind)
                self.prepare_extra_index(
                    self.indexes, metadata_inspector, db_schema_name, metadata.bind, logger=[]
                )
            except exc.NoInspectionAvailable:
                pass

    def load_file(
        self,
        file_name,
        batch_size=100,
        **kwargs,
    ):
        """Import a file into the database table, returns the last record, if available"""

        if self.dataset_table is None:
            raise ValueError("Import needs to be initialized with table info")
        data_generator = self.parse_records(
            file_name,
            self.dataset_table,
            self.db_table_name,
            **{"fields_provenances": self.fields_provenances, **kwargs},
        )
        self.logger.log_start(file_name, size=batch_size)
        num_imported = 0
        insert_statements = {
            table_name: table.insert() for table_name, table in self.tables.items()
        }

        last_record = None
        for records in chunked(data_generator, size=batch_size):
            # every record is keyed on tablename + inside there is a list
            for table_name, insert_statement in insert_statements.items():
                table_records = reduce(
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
            # main table is keys on tablename and only has one row
            last_record = records[-1][self.db_table_name][0]

        self.logger.log_done(num_imported)
        return last_record

    def parse_records(self, filename, dataset_table, db_table_name=None, **kwargs):
        """Yield all records from the filename"""
        raise NotImplementedError()

    def prepare_tables(self, tables, truncate=False):
        """Create the tables if needed"""
        for table in tables.values():
            if isinstance(table, Table):
                if not table.exists():
                    table.create()
                elif truncate:
                    self.engine.execute(table.delete())

    def prepare_extra_index(self, indexes, inspector, db_schema_name, engine, logger=None):
        """Create extra indexes on identifiers columns in base tables
        and identifier columns in n:m tables, if not exists"""

        # setup logger
        logger = LogfileLogger(logger) if logger else CliLogger()

        # In the indexes dict, the table name of each index, is stored in the key
        target_table_name = list(indexes.keys())

        # get current DB indexes on table
        current_db_indexes = set()
        for table in target_table_name:
            db_indexes = inspector.get_indexes(table, schema=db_schema_name)
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
        for index in indexes_to_create:
            try:
                logger.log_warning(f"Index '{index}' not found...creating")
                schema_indexes_objects[index].create(bind=engine)

            except AttributeError as e:
                logger.log_error(
                    f"Error creating index '{index}' for '{target_table_name}', error: {e}"
                )
                continue


class CliLogger:
    def __index__(self, batch_size):
        self.batch_size = batch_size

    def log_start(self, file_name, size):
        print(f"Importing data [each dot is {size} records]: ", end="", flush=True)

    def log_progress(self, num_imported):
        print(".", end="", flush=True)

    def log_error(self, msg, *args):
        print(msg % args)

    def log_warning(self, msg, *args):
        print(msg % args)

    def log_done(self, num_imported):
        print(f" Done importing {num_imported} records", flush=True)


class LogfileLogger(CliLogger):
    def __init__(self, logger):
        self.logger = logger

    def log_start(self, file_name, size):
        self.logger.info("Importing %s with %d records each:", file_name, size)

    def log_progress(self, num_imported):
        self.logger.info("- imported %d records", num_imported)

    def log_error(self, msg, *args):
        self.logger.error(msg, *args)

    def log_warning(self, msg, *args):
        self.logger.warning(msg, *args)

    def log_done(self, num_imported):
        self.logger.info("Done")


def table_factory(
    dataset_table: DatasetTableSchema,
    metadata: Optional[MetaData] = None,
    db_table_name: Optional[str] = None,
    db_schema_name: Optional[str] = None,
) -> Dict[str, Table]:
    """Generate one or more SQLAlchemy Table objects to work with the JSON Schema

    :param dataset_table: The Amsterdam Schema definition of the table
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
    :param db_table_name: Optional table name, which is otherwise inferred from the schema name.
    :param db_schema_name: Optional database schema name, which is otherwise None
        and defaults to public.

    The returned tables are keyed on the name of the table. The same goes for the incoming data,
    so during creation or records, the data can be associated with the correct table.
    """
    with_postfix = db_table_name is None
    if db_table_name is None:
        db_table_name = dataset_table.db_name()

    db_table_description = dataset_table.description

    metadata = metadata or MetaData()
    sub_tables = {}
    columns = []

    for field in dataset_table.fields:
        if field.type.endswith("#/definitions/schema"):
            continue
        field_name = to_snake_case(field.name)
        dataset_table_id = to_snake_case(dataset_table.id)
        dataset_table_name = to_snake_case(dataset_table.name)
        field_description = field.description
        sub_columns = []

        try:
            if field.is_array_of_objects:

                sub_table_id = shorten_name(
                    f"{db_table_name}_{field_name}", with_postfix=with_postfix
                )

                if field.is_nested_table:
                    # When the identifier is compound, we can assume
                    # that an extra 'id' field will be available, because
                    # Django cannot live without it.
                    id_field = dataset_table.identifier
                    id_field_name = "id" if len(id_field) > 1 else id_field[0]
                    fk_column = f"{db_table_name}.{id_field_name}"
                    sub_columns = [
                        Column("id", Integer, primary_key=True),
                        Column("parent_id", ForeignKey(fk_column, ondelete="CASCADE")),
                    ]

                elif field.is_through_table:

                    # We need a 'through' table for the n-m relation
                    sub_columns = [
                        Column(
                            f"{dataset_table_name}_id",
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
                        for id_field in dataset_table.get_fields_by_id(*dataset_table.identifier):
                            sub_columns.append(
                                Column(
                                    f"{dataset_table_id}_{to_snake_case(id_field.name)}",
                                    fetch_col_type(id_field),
                                )
                            )

                # Fields for either the nested or the through table
                for sub_field in field.sub_fields:
                    sub_columns.append(
                        Column(
                            to_snake_case(sub_field.name),
                            fetch_col_type(sub_field),
                        )
                    )

                sub_tables[sub_table_id] = Table(
                    sub_table_id,
                    metadata,
                    *sub_columns,
                    extend_existing=True,
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
        columns.append(
            Column(f"{field_name}{id_postfix}", col_type, comment=field_description, **col_kwargs)
        )

    return {
        db_table_name: Table(
            db_table_name, metadata, comment=db_table_description, schema=db_schema_name, *columns
        ),
        **sub_tables,
    }


def index_factory(
    dataset_table: DatasetTableSchema,
    ind_extra_index: bool,
    metadata: Optional[MetaData] = None,
    db_table_name: Optional[str] = None,
    db_schema_name: Optional[str] = None,
    logger: Optional[LogfileLogger] = None,
) -> Dict[str, Index]:
    """Generate one or more SQLAlchemy Index objects to work with the JSON Schema

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

    index = {}
    metadata = metadata or MetaData()
    logger = LogfileLogger(logger) if logger else CliLogger()

    if db_table_name is None:
        db_table_name = dataset_table.db_name()

    table_object = f"{db_schema_name}.{db_table_name}" if db_schema_name else db_table_name

    try:
        table_object = metadata.tables[table_object]
    except KeyError as ex:
        logger.log_error(f"{table_object} cannot be found.", repr(ex))

    indexes_to_create = []

    def make_hash_value(index_name):
        """
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

    def define_fk_index():
        """creates an index on the columns that refer to another table as
        a child (foreign key reference)"""

        if dataset_table.get_fk_fields():

            for field in dataset_table.get_fk_fields():
                field_name = f"{to_snake_case(field)}_id"
                index_name = f"{db_table_name}_{field_name}_idx"
                if len(index_name) > MAX_TABLE_NAME_LENGTH:
                    index_name = make_hash_value(index_name)
                indexes_to_create.append(Index(index_name, table_object.c[field_name]))

        # add Index objects to create
        index[db_table_name] = indexes_to_create

    def define_identifier_index():
        """creates index based on the 'identifier' specification in the Amsterdam schema"""

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
                    logger.log_error(f"{e.__str__} on {dataset_table.id}.{identifier_column}")
                    continue

        indexes_to_create.append(
            Index(f"{db_table_name}_identifier_idx", *identifier_column_snaked)
        )

        # add Index objects to create
        index[db_table_name] = indexes_to_create

    def define_throughtable_index():
        """creates index(es) on the many-to-many table based on 'relation' specification
        in the Amsterdam schema
        """

        for table in dataset_table.get_through_tables_by_id():

            through_columns = []
            indexes_to_create = []

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

                # Now check complementary field if they start with
                # the name of the relation field
                for field in table.fields:
                    if not field.relation:
                        snakecased_fieldname = to_snake_case(field.name)
                        if snakecased_fieldname.startswith(tuple(relation_field_names)):
                            through_columns.append(snakecased_fieldname)

            # create the Index objects
            if through_columns:
                table_id = table.db_name()

                try:
                    table_object = metadata.tables[table_id]

                except KeyError:
                    logger.log_error(
                        f"Unable to create Indexes {table_id}." f"Table not found in DB."
                    )
                    continue

                for column in through_columns:
                    index_name = table_id + "_" + column + TABLE_INDEX_POSTFIX
                    if len(index_name) > MAX_TABLE_NAME_LENGTH:
                        index_name = make_hash_value(index_name)
                    try:
                        indexes_to_create.append(Index(index_name, table_object.c[column]))
                    except KeyError as e:
                        logger.log_error(
                            f"{e.__str__}:{table_id}.{column} not found in {table_object.c}"
                        )
                        continue

                # add Index objects to create
                index[table_id] = indexes_to_create

    if ind_extra_index:
        define_identifier_index()
        define_throughtable_index()
        define_fk_index()

    return index
