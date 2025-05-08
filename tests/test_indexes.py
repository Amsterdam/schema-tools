from __future__ import annotations

from sqlalchemy import create_engine, inspect
from sqlalchemy.sql.ddl import CreateSchema

from schematools import MAX_TABLE_NAME_LENGTH, TABLE_INDEX_POSTFIX
from schematools.importer.base import BaseImporter
from schematools.types import DatasetSchema


def test_index_creation(engine, db_schema):
    """Prove that identifier index is created based on schema specificiation."""
    test_data = {
        "type": "dataset",
        "id": "test",
        "title": "test table",
        "description": "test table",
        "crs": "EPSG:28992",
        "versions": {
            "v1": {
                "version": "0.0.1",
                "status": "beschikbaar",
                "tables": [
                    {
                        "id": "test",
                        "type": "table",
                        "version": "1.0.0",
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
        },
    }

    dataset_schema = DatasetSchema.from_dict(test_data)
    index_names = set()

    for table in test_data["versions"]["v1"]["tables"]:
        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

        engine2 = create_engine(engine.url, client_encoding="UTF-8")
        inspector = inspect(engine2)
        table_db_name = f"{test_data['id']}_{table['id']}_v1"  # test_test
        indexes = inspector.get_indexes(table_db_name, schema=None)
        index_names.update(index["name"] for index in indexes)

    assert index_names == {
        "test_test_v1_identifier_idx",
        "test_test_v1_geometry_idx",
    }


def test_index_troughtables_creation(engine, db_schema):
    """Prove that many-to-many table indexes are created based on schema specification."""
    test_data = {
        "type": "dataset",
        "id": "test",
        "title": "TEST",
        "status": "niet_beschikbaar",
        "version": "0.0.1",
        "is_default_version": "true",
        "crs": "EPSG:28992",
        "identifier": "identificatie",
        "versions": {
            "v1": {
                "version": "0.0.1",
                "status": "beschikbaar",
                "tables": [
                    {
                        "id": "test_1",
                        "type": "table",
                        "version": "1.0.0",
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
                                "some_random_name": {
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
                        "type": "table",
                        "version": "1.0.0",
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
            },
        },
    }

    dataset_schema = DatasetSchema(test_data)
    indexes_names = set()

    for table in test_data["versions"]["v1"]["tables"]:
        importer = BaseImporter(dataset_schema, engine)
        # the generate_table and create index
        importer.generate_db_objects(
            table["id"],
            ind_tables=True,
            ind_extra_index=True,
        )

    for dataset_table in dataset_schema.tables:
        for field in dataset_table.fields:
            if field.is_through_table:
                engine2 = create_engine(engine.url, client_encoding="UTF-8")
                inspector = inspect(engine2)
                indexes = inspector.get_indexes(field.through_table.db_name, schema=None)
                indexes_names.update(index["name"] for index in indexes)

    assert indexes_names == {
        "6a5852e97fadf4a1a08cf016e1f6ef37d733a937_idx",
        "public.test_test_1_heeft_onderzoeken_v1_test_1_id_idx",
        "public.test_test_1_some_random_name_v1_some_random_name_id_idx",
        "public.test_test_1_some_random_name_v1_test_1_id_idx",
    }


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
        "versions": {
            "v1": {
                "version": "0.0.1",
                "status": "beschikbaar",
                "tables": [
                    {
                        "id": "parent_test",
                        "type": "table",
                        "version": "1.0.0",
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
                        "type": "table",
                        "version": "1.0.0",
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
        },
    }

    dataset_schema = DatasetSchema(test_data)
    table = dataset_schema.get_table_by_id("child_test")

    importer = BaseImporter(dataset_schema, engine)
    # the generate_table and create index
    importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

    engine2 = create_engine(engine.url, client_encoding="UTF-8")
    inspector = inspect(engine2)
    indexes = inspector.get_indexes(table.db_name, schema=None)
    indexes_name = {index["name"] for index in indexes}
    assert indexes_name == {
        "test_child_test_v1_identifier_idx",
        "test_child_test_v1_fk_column_reference_id_idx",
    }


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
        "versions": {
            "v1": {
                "version": "0.0.1",
                "status": "beschikbaar",
                "tables": [
                    {
                        "id": "parent_test_size",
                        "type": "table",
                        "version": "1.0.0",
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
                        "type": "table",
                        "version": "1.0.0",
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
        },
    }

    data = test_data
    dataset_schema = DatasetSchema(data)
    table = dataset_schema.get_table_by_id("child_test_size")

    importer = BaseImporter(dataset_schema, engine)
    # the generate_table and create index
    importer.generate_db_objects(table["id"], ind_tables=True, ind_extra_index=True)

    engine2 = create_engine(engine.url, client_encoding="UTF-8")
    inspector = inspect(engine2)
    indexes = inspector.get_indexes(f"{data['id']}_{table['id']}_v1", schema=None)
    indexes_name = []

    for index in indexes:
        indexes_name.append(index["name"])
    for index_name in indexes_name:
        assert len(index_name) <= (MAX_TABLE_NAME_LENGTH - len(TABLE_INDEX_POSTFIX))


def test_index_creation_db_schema2(engine, stadsdelen_schema):
    """Prove that indexes are created within given database schema on table."""
    # create DB schema
    with engine.begin() as conn:
        conn.execute(CreateSchema("schema_foo_bar", if_not_exists=True))

    importer = BaseImporter(stadsdelen_schema, engine)
    importer.generate_db_objects(
        "stadsdelen", "schema_foo_bar", ind_tables=True, ind_extra_index=True
    )
    inspector = inspect(engine)
    indexes = inspector.get_indexes(
        f"{stadsdelen_schema['id']}_stadsdelen_v1", schema="schema_foo_bar"
    )
    index_names = {index["name"] for index in indexes}
    assert index_names == {"stadsdelen_stadsdelen_v1_identifier_idx"}
