__doc__ = """

Direct de tabel bijwerken, of via een tussenstap (met validaties)
Validaties wschl. eerder in het proces (bij de ingest)

"""
import json
from typing import Optional
from sqlalchemy import MetaData, Table, Column
from schematools.types import DatasetTableSchema
from schematools.utils import to_snake_case
from schematools.importer import fetch_col_type, get_table_name

metadata = MetaData()


def fetch_insert_data(event_data):
    return event_data["entity"]


def fetch_update_data(event_data):
    update_data = {}
    for modification in event_data["modifications"]:
        # XXX skip geometrie for now, has geojson format (no wkt)
        if modification["key"] == "geometrie":
            continue
        update_data[modification["key"]] = modification["new_value"]
    return update_data


EVENT_TYPE_MAPPING = {
    "ADD": ("insert", False, fetch_insert_data),
    "MODIFY": ("update", True, fetch_update_data),
    "DELETE": ("delete", True, None),
}


class EventsProcessor:
    def __init__(
        self,
        dataset_table: DatasetTableSchema,
        srid,
        connection,
        local_metadata=False,
        truncate=False,
    ):
        self.dataset_table = dataset_table
        self.srid = srid
        self.conn = connection
        _metadata = MetaData() if local_metadata else metadata  # mainly for testing
        _metadata.bind = connection.engine
        self.table = table_factory(self.dataset_table, _metadata)
        if not self.table.exists():
            self.table.create()
        elif truncate:
            self.conn.execute(self.table.delete())
        self.identifier = dataset_table.identifier
        self.has_compound_key = dataset_table.has_compound_key
        self.geo_fields = geo_fields = []
        for field in dataset_table.fields:
            if field.is_geo:
                geo_fields.append(field.name)

    def process_event(self, identification, event_data):
        """ Do inserts/updates/deletes """

        db_operation_name, needs_select, data_fetcher = EVENT_TYPE_MAPPING[
            event_data["_event_type"]
        ]
        row = None
        if data_fetcher is not None:
            row = data_fetcher(event_data)
            for field_name in self.geo_fields:
                geo_value = row.get(field_name)
                if geo_value is not None:
                    row[field_name] = f"SRID={self.srid};{geo_value}"
            # Add id field
            # id_value = ".".join(str(row[fn]) for fn in self.identifier)
            if self.has_compound_key:
                row["id"] = identification

        db_operation = getattr(self.table, db_operation_name)()
        if needs_select:
            db_operation = db_operation.where(self.table.c.id == identification)
        self.conn.execute(db_operation, row if row is not None else {})

    def load_events_from_file(self, events_path):
        """ Helper method, primarily used for testing """

        with open(events_path) as ef:
            for line in ef:
                if line.strip():
                    identification, data_str = line.split("|", maxsplit=1)
                    event_data = json.loads(data_str)
                    self.process_event(identification, event_data)


def table_factory(
    dataset_table: DatasetTableSchema,
    metadata: Optional[MetaData] = None,
) -> Table:
    """Generate thie SQLAlchemy Table object to work with the JSON Schema

    :param dataset_table: The Amsterdam Schema definition of the table
    :param metadata: SQLAlchemy schema metadata that groups all tables to a single connection.

    The returned tables are keyed on the name of the table. The same goes for the incoming data,
    so during creation or records, the data can be associated with the correct table.
    """
    db_table_name = get_table_name(dataset_table)

    metadata = metadata or MetaData()
    columns = []
    for field in dataset_table.fields:
        if (
            field.type.endswith("#/definitions/schema")
            or field.relation
            or field.nm_relation
        ):
            continue
        field_name = to_snake_case(field.name)

        try:
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
        columns.append(Column(f"{field_name}{id_postfix}", col_type, **col_kwargs))

    return Table(db_table_name, metadata, *columns)
