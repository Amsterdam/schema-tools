from itertools import islice
from typing import Optional

from schematools.types import DatasetSchema, DatasetTableSchema
from geoalchemy2 import Geometry
from sqlalchemy import Boolean, Column, Float, Integer, MetaData, String, Table

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
    make_chunk = lambda: list(islice(gen, size))
    return iter(make_chunk, [])


class BaseImporter:
    """Base importer that holds common data."""

    def __init__(self, dataset_schema, engine):
        self.engine = engine
        self.dataset_schema = dataset_schema
        self.srid = dataset_schema["crs"].split(":")[-1]

    def get_db_table_name(self, table_name):
        dataset_table = self.dataset_schema.get_table_by_id(table_name)
        return get_table_name(dataset_table)

    def load_file(
        self,
        file_name,
        table_name,
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
        table = table_factory(
            dataset_table, metadata=metadata, db_table_name=db_table_name
        )
        self.prepare_table(table, truncate=truncate)

        data_generator = self.parse_records(file_name, **kwargs)
        print("Importing data [each dot is 100 records]: ", end="", flush=True)
        num_imported = 0

        for records in chunked(data_generator, 100):
            self.engine.execute(pg_table.insert(), records)
            print(".", end="", flush=True)
            num_imported += len(records)

        print(f" Done importing {num_imported} records", flush=True)

    def parse_records(self, filename, **kwargs):
        """Yield all records from the filename"""
        raise NotImplementedError()

    def prepare_table(self, table, truncate=False):
        """Create the table if needed"""
        if not table.exists():
            table.create()
        elif truncate:
            print(table.delete())
            self.engine.execute(table.delete())

def table_factory(
    dataset_table: DatasetTableSchema,
    metadata: Optional[MetaData] = None,
    db_table_name=None,
) -> Table:
    """Generate an SQLAlchemy Table object to work with the JSON Schema

    :param dataset_table: The Amsterdam Schema definition of the table
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.
    :param db_table_name: Optional table name, which is otherwise inferred from the schema name.
    """
    if db_table_name is None:
        db_table_name = get_table_name(dataset_table)

    columns = []
    for field in dataset_table.fields:
        if field.type.endswith("#/definitions/schema"):
            continue

        try:
            col_type = JSON_TYPE_TO_PG[field.type]
        except KeyError:
            raise NotImplementedError(
                f'Import failed at "{field.name}": {dict(field)!r}\n'
                f"Field type '{field.type}' is not implemented."
            ) from None

        col_kwargs = {}
        if field.name == "id":
            col_kwargs["primary_key"] = True
            col_kwargs["nullable"] = True

        columns.append(Column(field.name, col_type, **col_kwargs))

    return Table(db_table_name, metadata or MetaData(), *columns)


def get_table_name(dataset_table: DatasetTableSchema) -> str:
    """Generate the database identifier for the table."""
    schema = dataset_table._parent_schema
    return f"{schema.id}_{dataset_table.id}".replace("-", "_")
