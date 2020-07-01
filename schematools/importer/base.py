from functools import reduce
from itertools import islice
import operator
from typing import Optional, Dict

from schematools.types import DatasetSchema, DatasetTableSchema
from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)

from . import get_table_name


JSON_TYPE_TO_PG = {
    "string": String,
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


def chunked(generator, size):
    """Read parts of the generator, pause each time after a chunk"""
    # Based on more-itertools. islice returns results until 'size',
    # iter() repeatedly calls make_chunk until the '[]' sentinel is returned.
    gen = iter(generator)
    make_chunk = lambda: list(islice(gen, size))  # NoQA
    return iter(make_chunk, [])


class BaseImporter:
    """Base importer that holds common data."""

    def __init__(self, dataset_schema: DatasetSchema, engine, logger=None):
        self.engine = engine
        self.dataset_schema = dataset_schema
        self.srid = dataset_schema["crs"].split(":")[-1]
        self.logger = LogfileLogger(logger) if logger else CliLogger()

    def get_db_table_name(self, table_name):
        dataset_table = self.dataset_schema.get_table_by_id(table_name)
        return get_table_name(dataset_table)

    def load_file(
        self,
        file_name,
        table_name,
        batch_size=100,
        db_table_name=None,
        truncate=False,
        **kwargs,
    ):
        """Import a file into the database table"""
        dataset_table = self.dataset_schema.get_table_by_id(table_name)
        if db_table_name is None:
            db_table_name = get_table_name(dataset_table)

        # Get a table to import into
        metadata = MetaData(bind=self.engine)
        tables = table_factory(
            dataset_table, metadata=metadata, db_table_name=db_table_name
        )
        self.prepare_tables(tables, truncate=truncate)

        data_generator = self.parse_records(
            file_name, dataset_table, db_table_name, **kwargs
        )
        self.logger.log_start(file_name, size=batch_size)
        num_imported = 0
        insert_statements = {
            table_name: table.insert() for table_name, table in tables.items()
        }

        for records in chunked(data_generator, size=batch_size):
            # every record is keyed on tablename + inside there is a list
            for table_name, insert_statement in insert_statements.items():
                table_records = reduce(
                    operator.add, [record.get(table_name, []) for record in records], []
                )
                self.engine.execute(insert_statement, table_records)
            num_imported += len(records)
            self.logger.log_progress(num_imported)

        self.logger.log_done(num_imported)

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

    def log_done(self, num_imported):
        print(f" Done importing {num_imported} records", flush=True)


class LogfileLogger(CliLogger):
    def __init__(self, logger):
        self.logger = logger

    def log_start(self, file_name, size):
        self.logger.info("Importing %s with %d records each:", file_name, size)

    def log_progress(self, num_imported):
        self.logger.info("- imported %d records", num_imported)

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

        # XXX still need to throw in some snakifying here and there
        try:
            nm_relation = field.nm_relation
            if nm_relation is not None:
                # We need a 'through' table for the n-m relation
                related_dataset, related_table = nm_relation.split(":")
                through_columns = [
                    Column(f"{dataset_table.id}_id", String,),
                    Column(f"{related_table}_id", String,),
                ]
                through_table_id = f"{db_table_name}_{field.name}"
                through_tables[through_table_id] = Table(
                    through_table_id, metadata, *through_columns,
                )

                continue

            col_type = JSON_TYPE_TO_PG[field.type]
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
        columns.append(Column(f"{field.name}{id_postfix}", col_type, **col_kwargs))

    return {db_table_name: Table(db_table_name, metadata, *columns), **through_tables}
