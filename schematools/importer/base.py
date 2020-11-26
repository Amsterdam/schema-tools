from collections import UserDict
from functools import reduce
from itertools import islice
import operator
from typing import Optional, Dict
from jsonpath_rw import parse
from sqlalchemy import (
    MetaData,
    inspect,
    Table,
    Column,
    ForeignKey,
    Integer,
    String,
    Index,
    exc,
)

from schematools import MAX_TABLE_LENGTH
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import to_snake_case
from . import get_table_name, fetch_col_type

metadata = MetaData()


def chunked(generator, size):
    """Read parts of the generator, pause each time after a chunk"""
    # Based on more-itertools. islice returns results until 'size',
    # iter() repeatedly calls make_chunk until the '[]' sentinel is returned.
    gen = iter(generator)
    make_chunk = lambda: list(islice(gen, size))  # NoQA
    return iter(make_chunk, [])


class JsonPathException(Exception):
    pass


class Row(UserDict):

    # class-level cache for jsonpath expressions
    _expr_cache = {}

    def __init__(self, *args, **kwargs):
        self.fields_provenances = {
            name: prov_name
            for prov_name, name in kwargs.pop("fields_provenances", {}).items()
        }
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        try:
            value = super().__getitem__(self._transform_key(key))
        except JsonPathException:
            value = self._fetch_value_for_jsonpath(key)
        return value

    def __delitem__(self, key):
        return super().__delitem__(self._transform_key(key))

    def _transform_key(self, key):
        if key in self.data:
            return key
        prov_key = self.fields_provenances.get(key)
        if prov_key is None:
            return key
        if prov_key.startswith("$"):
            raise JsonPathException()
        return prov_key

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
        if not matches:
            raise ValueError(f"No content for {prov_key}")
        value = matches[0].value
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
        self.tables = []
        self.pk_values_lookup = {}
        self.pk_colname_lookup = {}
        self.logger = LogfileLogger(logger) if logger else CliLogger()

    def get_db_table_name(self, table_name):
        dataset_table = self.dataset_schema.get_table_by_id(table_name)
        return get_table_name(dataset_table)

    def fetch_fields_provenances(self, dataset_table):
        """ Create mapping from provenance to camelcased fieldname """
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
            value = record[pk_name]
            if value not in values_lookup and value not in this_batch_pk_values:
                yield record
            else:
                self.logger.log_warning(
                    "Duplicate record for %s, with %s = %s", table_name, pk_name, value
                )
            this_batch_pk_values.add(value)
            values_lookup.add(value)

    def create_pk_lookup(self, tables):
        """ Generate a lookup to avoid primary_key clashes """
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
                pks = set(
                    [getattr(r, pk_name) for r in self.engine.execute(table.select())]
                )
                self.pk_values_lookup[table_name] = pks

    def generate_db_objects(
        self,
        table_name,
        db_table_name=None,
        truncate=False,
        ind_tables=True,
        ind_identifier_index=True,
    ):
        """Generate the tablemodels and tables and / or index on identifier as specified in the JSON data schema.
            As default both table and index creation are set to True.
        """

        if ind_tables or ind_identifier_index:
            self.dataset_table = self.dataset_schema.get_table_by_id(table_name)

            # Collect provenance info for easy re-use
            self.fields_provenances = self.fetch_fields_provenances(self.dataset_table)
            self.db_table_name = db_table_name
            if db_table_name is None:
                self.db_table_name = get_table_name(self.dataset_table)
            # Bind the metadata
            metadata.bind = self.engine
            # Get a table to import into
            self.tables = table_factory(
                self.dataset_table, metadata=metadata, db_table_name=self.db_table_name
            )

        if ind_tables:
            self.prepare_tables(self.tables, truncate=truncate)
            self.create_pk_lookup(self.tables)

        if ind_identifier_index:
            # inspect on metadata.bind needed for index creation (prepare_identifier_index)
            try:
                metadata_inspector = inspect(metadata.bind)
            except exc.NoInspectionAvailable as e:
                metadata_inspector = []
            self.prepare_identifier_index(
                self.tables, metadata_inspector, metadata.bind
            )

    def load_file(
        self, file_name, batch_size=100, **kwargs,
    ):
        """Import a file into the database table, returns the last record, if available """

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
                        insert_statement, table_records,
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

    def prepare_identifier_index(self, tables, inspector, engine):
        """ Create indexes if needed """

        table_name = None
        table_object = None
        ind_identifier_index = False

        for object_name, object in tables.items():

            if isinstance(object, Table):
                # get the table name and Table instance
                # the Table instance is assigned to the generic var name "table_object"
                # which is referenced by the Index SQLalchemy class instantiation and
                # expects a Table instance as it's value
                table_name = object_name
                table_object = object

            # check for presence of the _identifier_idx Index object
            if object_name == f"{table_name}_identifier_idx":

                try:
                    # fetch indexes based on specification in DB
                    indexes = inspector.get_indexes(table_name, schema=None)
                    for index in indexes:
                        if index.get("name", None) == object_name:
                            ind_identifier_index = True

                    if not ind_identifier_index:
                        print("Identifier index not found...start creating", object)
                        # eval is used to execute string to code (which is used to make it dynamicly generated)
                        eval(object).create(bind=engine)

                except AttributeError as e:
                    print(f"Error on identifier index for {object_name}:", e)
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
    db_table_name=None,
) -> Dict[str, Table]:
    """Generate one or more SQLAlchemy Table objects to work with the JSON Schema

    :param dataset_table: The Amsterdam Schema definition of the table
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
    :param db_table_name: Optional table name, which is otherwise inferred from the schema name.

    The returned tables are keyed on the name of the table. The same goes for the incoming data,
    so during creation or records, the data can be associated with the correct table.
    """
    if db_table_name is None:
        db_table_name = get_table_name(dataset_table)

    metadata = metadata or MetaData()
    sub_tables = {}
    columns = []
    identifier_index = {}

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
                        Column("parent_id", ForeignKey(fk_column, ondelete="CASCADE")),
                    ]

                elif field.is_through_table:
                    # We need a 'through' table for the n-m relation
                    sub_columns = [
                        Column(f"{dataset_table.id}_id", String,),
                        Column(f"{field_name}_id", String,),
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
                    colname_prefix = f"{field_name}_" if field.is_through_table else ""
                    sub_columns.append(
                        Column(
                            f"{colname_prefix}{to_snake_case(sub_field.name)}",
                            fetch_col_type(sub_field),
                        )
                    )

                sub_tables[sub_table_id] = Table(sub_table_id, metadata, *sub_columns,)

                continue

            col_type = fetch_col_type(field)

            if dataset_table.identifier and field.name in dataset_table.identifier:
                identifier_index = define_identifier_index(db_table_name, dataset_table)

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

    return {
        db_table_name: Table(db_table_name, metadata, *columns),
        **identifier_index,
        **sub_tables,
    }


def define_identifier_index(
    db_table_name: str, dataset_table: DatasetTableSchema
) -> Dict:
    """Generate one or more SQLAlchemy Index objects to work with the JSON Schema

    :param db_table_name: The full table name: [name_dataset]_ [name_table]
    :param dataset_table: The Amsterdam Schema definition of the table

    In the JSON Schema definition of the table, an identifier arry maybe definied.
    I.e. "identifier": ["identificatie", "volgnummer"]

    This is frequently used in temporal tables where versions of data are present (history).

    An index on the identifier is needed in order to maximize performance when SQL joining to the vesionized record.
    """
    identifier_column_snaked = []
    index = {}

    if not identifier_column_snaked:

        # precautionary measure: If camelCase, translate to snake_case, so column(s) can be found in table
        # table_object is a generic name which contains a instance of SQLalchemy Table class, which references to the DB table
        # the letter 'c' between table_object and field of identifier, is a mandatory element of SQLalchemy to specifiy the
        # fields that will be part of the index creation.
        for identifier_column in dataset_table.identifier:
            identifier_column_snaked.append(
                f"table_object.c.{to_snake_case(identifier_column)}"
            )

        index[
            f"{db_table_name}_identifier_idx"
        ] = f"Index('{db_table_name}_identifier_idx', {','.join(identifier_column_snaked)})"

    return index
