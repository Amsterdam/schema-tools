# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`

Currently, the following cli commands are available:

    - schema fetch tablenames
    - schema fetch schema
    - schema introspect <dataset-id> <list-of-tablenames>
    - schema generate arschema
    - schema generate records
    - schema validate

The tools expect either a DATABASE_URL environment variable or a command-line
option `--db-url` with a DSN.

The output is a json-schema output according to the Amsterdam schemas
definition for the tables that are being processed.

## schema introspect

The --prefix argument controls whether table prefixes are removed in the
schema, because that is required for Django models.

As example we can generate a BAG schema. Point DATABASE_URL to bag_v11 database and then run :

    schema fetch tablenames | sort | awk '/^bag_/{print}' | xargs schema introspect bag --prefix bag_ | jq

The **jq** formats it nicely and it can be redirected to the correct directory
in the schemas repository directly.

## schema fetch schema and schema generate arschema

Amsterdam schema is expressed as jsonschema. However, to make it easier for people with a
more relational mind- or toolset it is possible to express amsterdam schema in a set of
relational tables. These tables are *meta_dataset*, *meta_table* and *meta_field*.

It is possible to convert a jsonschema into the relational table structure and vice-versa.

This command converts a dataset from an existing dataset in jsonschema format:

    schema generate arschema <id of dataset>

To convert from relational tables back to jsonschema:

    schema fetch schema <id of dataset>
