from collections import UserDict
from functools import reduce
from itertools import islice
import operator
from typing import Optional, Dict
from jsonpath_rw import parse

from schematools import MAX_TABLE_LENGTH
from schematools.types import DatasetSchema, DatasetTableSchema
from schematools.utils import to_snake_case
from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Date,
    Time,
    DateTime,
    inspect,
)
from . import get_table_name

FORMAT_MODELS_LOOKUP = {
    "date": Date,
    "time": Time,
    "date-time": DateTime,
}
metadata = MetaData()

JSON_TYPE_TO_PG = {
    "string": String,
    "object": String,
    "boolean": Boolean,
    "integer": Integer,
    "number": Float,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.0#/definitions/schema": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/id": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/class": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/dataset": String,
    "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema": String,
    "https://geojson.org/schema/Geometry.json": Geometry(
        geometry_type="GEOMETRY", srid=28992
    ),
    "https://geojson.org/schema/Point.json": Geometry(
        geometry_type="POINT", srid=28992
    ),
    "https://geojson.org/schema/Polygon.json": Geometry(
        geometry_type="POLYGON", srid=28992
    ),
    "https://geojson.org/schema/MultiPolygon.json": Geometry(
        geometry_type="MULTIPOLYGON", srid=28992
    ),
    "https://geojson.org/schema/MultiPoint.json": Geometry(
        geometry_type="MULTIPOINT", srid=28992
    ),
    "https://geojson.org/schema/LineString.json": Geometry(
        geometry_type="LINESTRING", srid=28992
    ),
    "https://geojson.org/schema/MultiLineString.json": Geometry(
        geometry_type="MULTILINESTRING", srid=28992
    ),
}


def fetch_col_type(field):
    col_type = JSON_TYPE_TO_PG[field.type]
    # XXX no walrus until we can go to python 3.8 (airflow needs 3.7)
    # if (field_format := field.format) is not None:
    field_format = field.format
    if field_format is not None:
        return FORMAT_MODELS_LOOKUP[field_format]
    return col_type


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

    def _fetch_value_for_jsonpath(self, key):
        prov_key = self.fields_provenances.get(key)
        top_element_name = prov_key.split(".")[1]
        expr = self._expr_cache.setdefault(prov_key, parse(prov_key))
        matches = expr.find({top_element_name: self.data[top_element_name]})
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
        """ We need relational snakecased fieldnames in the records
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
                self.logger.log_error(
                    "Duplicate record for %s, with %s = %s", table_name, pk_name, value
                )
            this_batch_pk_values.add(value)

    def create_pk_lookup(self, tables):
        """ Generate a lookup to avoid primary_key clashes """
        for table_name, table in tables.items():
            pk_columns = inspect(table).primary_key.columns
            # nm tables do not have a PK
            if not pk_columns:
                return
            # We assume a single PK (because of Django)
            pk_name = pk_columns.values()[0].name
            self.pk_colname_lookup[table_name] = pk_name
            pks = set(
                [getattr(r, pk_name) for r in self.engine.execute(table.select())]
            )
            self.pk_values_lookup[table_name] = pks

    def generate_tables(self, table_name, db_table_name=None, truncate=False):
        """ Generate the tablemodels and tables
        """

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
        self.prepare_tables(self.tables, truncate=truncate)
        self.create_pk_lookup(self.tables)

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
            if not table.exists():
                table.create()
            elif truncate:
                print(table.delete())
                self.engine.execute(table.delete())


class CliLogger:
    def __index__(self, batch_size):
        self.batch_size = batch_size

    def log_start(self, file_name, size):
        print(f"Importing data [each dot is {size} records]: ", end="", flush=True)

    def log_progress(self, num_imported):
        print(".", end="", flush=True)

    def log_error(self, msg, *args):
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
    through_tables = {}
    columns = []
    for field in dataset_table.fields:
        if field.type.endswith("#/definitions/schema"):
            continue
        field_name = to_snake_case(field.name)

        try:
            nm_relation = field.nm_relation
            if nm_relation is not None:
                field_name = to_snake_case(field.name)
                # We need a 'through' table for the n-m relation
                _, related_table = [
                    to_snake_case(part) for part in nm_relation.split(":")
                ]
                through_columns = [
                    Column(f"{dataset_table.id}_id", String,),
                    Column(f"{related_table}_id", String,),
                ]
                # Through table means that in schema definition there is
                # an object with one or more fields defining the relation
                if field.is_through_table:
                    for through_field_name, through_field_type_info in field["items"][
                        "properties"
                    ].items():
                        through_columns.append(
                            Column(
                                through_field_name,
                                JSON_TYPE_TO_PG[through_field_type_info["type"]],
                            )
                        )

                through_table_id = f"{db_table_name}_{field_name}"[:MAX_TABLE_LENGTH]
                through_tables[through_table_id] = Table(
                    through_table_id, metadata, *through_columns,
                )

                continue

            # col_type = JSON_TYPE_TO_PG[field.type]
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

        id_postfix = "_id" if field.relation else ""
        columns.append(Column(f"{field_name}{id_postfix}", col_type, **col_kwargs))

    return {db_table_name: Table(db_table_name, metadata, *columns), **through_tables}
