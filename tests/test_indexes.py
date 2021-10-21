from sqlalchemy import MetaData, create_engine, inspect

from schematools import MAX_TABLE_NAME_LENGTH, TABLE_INDEX_POSTFIX
from schematools.importer.base import BaseImporter
from schematools.types import DatasetSchema, SchemaType


def test_index_creation(engine, db_schema):
    """Prove that identifier index is created based on schema specificiation."""
    test_data = {
        "schemaType": "dataset",
        "id": "test",
        "title": "test table",
        "status": "beschikbaar",
        "description": "test table",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "tables": [
            {
                "id": "test",
                "schemaType": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema", "id"],
                    "display": "id",
                    "identifier": ["col1", "col2"],
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "integer"},
                        "geometry": {"$ref": "https://geojson.org/schema/Geometry.json"},
                        "col1": {"type": "string"},
                        "col2": {"type": "string"},
                        "col3": {"type": "string"},
                    },
                },
            }
        ],
    }

    data = test_data
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    ind_index_exists = False

    for table in data["tables"]:
        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

        conn = create_engine(engine.url, client_encoding="UTF-8")
        meta_data = MetaData(bind=conn)
        meta_data.reflect()
        metadata_inspector = inspect(meta_data.bind)
        indexes = metadata_inspector.get_indexes(
            f"{parent_schema['id']}_{table['id']}", schema=None
        )
        indexes_name = []

        for index in indexes:
            indexes_name.append(index["name"])
        if any("identifier_idx" in i for i in indexes_name):
            ind_index_exists = True
        assert ind_index_exists


def test_index_troughtables_creation(engine, db_schema):
    """Prove that many-to-many table indexes are created based on schema specification.

    A NM relation with a very long name has deliberatly added. The truncation of index
    names should avoid failing tests.
    """
    test_data = {
        "schemaType": "dataset",
        "id": "test",
        "title": "TEST",
        "status": "niet_beschikbaar",
        "version": "0.0.1",
        "is_default_version": "true",
        "crs": "EPSG:28992",
        "identifier": "identificatie",
        "tables": [
            {
                "id": "test_1",
                "schemaType": "table",
                "temporal": {
                    "identifier": "volgnummer",
                    "dimensions": {"geldigOp": ["beginGeldigheid", "eindGeldigheid"]},
                },
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "identifier": ["identificatie", "volgnummer"],
                    "required": ["schema", "id", "identificatie", "volgnummer"],
                    "display": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "identificatie": {"type": "string"},
                        "volgnummer": {"type": "integer"},
                        "beginGeldigheid": {"type": "string", "format": "date-time"},
                        "eindGeldigheid": {"type": "string", "format": "date-time"},
                        "heeftOnderzoeken": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "identificatie": {"type": "string"},
                                    "volgnummer": {"type": "integer"},
                                },
                            },
                            "relation": "test:test_2",
                        },
                        "lang456789012345678901234567890123456789verylongtail": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "identificatie": {"type": "string"},
                                    "volgnummer": {"type": "integer"},
                                },
                            },
                            "relation": "test:test_2",
                        },
                    },
                    "mainGeometry": "geometrie",
                },
            },
            {
                "id": "test_2",
                "schemaType": "table",
                "temporal": {
                    "identifier": "volgnummer",
                    "dimensions": {"geldigOp": ["beginGeldigheid", "eindGeldigheid"]},
                },
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "identifier": ["identificatie", "volgnummer"],
                    "required": ["schema", "id", "identificatie", "volgnummer"],
                    "display": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "identificatie": {"type": "string"},
                        "volgnummer": {"type": "integer"},
                        "beginGeldigheid": {"type": "string", "format": "date-time"},
                        "eindGeldigheid": {"type": "string", "format": "date-time"},
                    },
                },
            },
        ],
    }

    data = test_data
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    indexes_name = []

    for table in data["tables"]:

        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(
            table["id"],
            ind_tables=True,
            ind_extra_index=True,
        )

    for table in data["tables"]:

        dataset_table = dataset_schema.get_table_by_id(table["id"])

        for table in dataset_table.get_through_tables_by_id():

            conn = create_engine(engine.url, client_encoding="UTF-8")
            meta_data = MetaData(bind=conn)
            meta_data.reflect()
            metadata_inspector = inspect(meta_data.bind)
            indexes = metadata_inspector.get_indexes(table.db_name(), schema=None)

            for index in indexes:
                indexes_name.append(index["name"])

    number_of_indexes = len(indexes_name)

    # Many-to-many tables must have at least one index
    assert number_of_indexes > 0


def test_fk_index_creation(engine, db_schema):
    """Prove that index is created on 1:N relational columns based on schema specificiation."""
    test_data = {
        "type": "dataset",
        "id": "test",
        "title": "test table",
        "status": "beschikbaar",
        "description": "test table",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "tables": [
            {
                "id": "parent_test",
                "schemaType": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema", "id"],
                    "identifier": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "string"},
                    },
                },
            },
            {
                "id": "child_test",
                "schemaType": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema"],
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "string"},
                        "fkColumnReference": {
                            "type": "string",
                            "relation": "test:parent_test",
                        },
                    },
                },
            },
        ],
    }

    data = test_data
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    ind_index_exists = False

    for table in data["tables"]:
        if table["id"] == "child_test":

            importer = BaseImporter(dataset_schema, engine)
            # the generate_table and create index
            importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

            conn = create_engine(engine.url, client_encoding="UTF-8")
            meta_data = MetaData(bind=conn)
            meta_data.reflect()
            metadata_inspector = inspect(meta_data.bind)
            indexes = metadata_inspector.get_indexes(
                f"{parent_schema['id']}_{table['id']}", schema=None
            )
            indexes_name = []

            for index in indexes:
                indexes_name.append(index["name"])
            if any("fk_column_reference" in i for i in indexes_name):
                ind_index_exists = True
            assert ind_index_exists


def test_size_of_index_name(engine, db_schema):
    """Prove that the size of the index name does not get too long.

    It should not exeed the size for Postgres database object names
    as defined in MAX_TABLE_NAME_LENGTH plus TABLE_INDEX_POSTFIX.
    """
    test_data = {
        "type": "dataset",
        "id": "test",
        "title": "test table",
        "status": "beschikbaar",
        "description": "test table",
        "version": "0.0.1",
        "crs": "EPSG:28992",
        "tables": [
            {
                "id": "parent_test_size",
                "schemaType": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema", "id"],
                    "identifier": "id",
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "string"},
                    },
                },
            },
            {
                "id": "child_test_size",
                "schemaType": "table",
                "schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": "false",
                    "required": ["schema"],
                    "properties": {
                        "schema": {
                            "$ref": (
                                "https://schemas.data.amsterdam.nl/schema@v1.1.1"
                                "#/definitions/schema"
                            )
                        },
                        "id": {"type": "string"},
                        "fkColumnReferenceWithAReallyLongRidiculousNameThatMustBeShortend": {
                            "type": "string",
                            "relation": "test:parent_test_size",
                        },
                    },
                },
            },
        ],
    }

    data = test_data
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)

    for table in data["tables"]:
        if table["id"] == "child_test_size":

            importer = BaseImporter(dataset_schema, engine)
            # the generate_table and create index
            importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

            conn = create_engine(engine.url, client_encoding="UTF-8")
            meta_data = MetaData(bind=conn)
            meta_data.reflect()
            metadata_inspector = inspect(meta_data.bind)
            indexes = metadata_inspector.get_indexes(
                f"{parent_schema['id']}_{table['id']}", schema=None
            )
            indexes_name = []

            for index in indexes:
                indexes_name.append(index["name"])
            for index_name in indexes_name:
                assert len(index_name) <= (MAX_TABLE_NAME_LENGTH - len(TABLE_INDEX_POSTFIX))


def test_index_creation_db_schema2(engine, stadsdelen_schema):
    """Prove that indexes are created within given database schema on table."""
    # create DB schema
    engine.execute("CREATE SCHEMA IF NOT EXISTS schema_foo_bar;")
    importer = BaseImporter(stadsdelen_schema, engine)
    importer.generate_db_objects(
        "stadsdelen", "schema_foo_bar", ind_tables=True, ind_extra_index=True
    )
    meta_data = MetaData(bind=engine)
    meta_data.reflect()
    metadata_inspector = inspect(meta_data.bind)
    parent_schema = SchemaType(stadsdelen_schema)
    indexes = metadata_inspector.get_indexes(
        f"{parent_schema['id']}_stadsdelen", schema="schema_foo_bar"
    )
    indexes_name = []
    for index in indexes:
        indexes_name.append(index["name"])
    if any("identifier_idx" in i for i in indexes_name):
        ind_index_exists = True
    assert ind_index_exists
