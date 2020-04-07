# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`

Currently, the following cli commands are available:

- schema import schema
- schema import ndjson
- schema show schema <dataset-id>
- schema show tablenames
- schema introspect db <dataset-id> <list-of-tablenames>
- schema introspect geojson <dataset-id> \*.geojson
- schema validate

The tools expect either a `DATABASE_URL` environment variable or a command-line option `--db-url` with a DSN.

The output is a json-schema output according to the Amsterdam schemas
definition for the tables that are being processed.

## Generate amsterdam schema from existing database tables

The --prefix argument controls whether table prefixes are removed in the
schema, because that is required for Django models.

As example we can generate a BAG schema. Point `DATABASE_URL` to `bag_v11` database and then run :

    schema show tablenames | sort | awk '/^bag_/{print}' | xargs schema introspect db bag --prefix bag_ | jq

The **jq** formats it nicely and it can be redirected to the correct directory
in the schemas repository directly.

## Express amsterdam schema information in relational tables

Amsterdam schema is expressed as jsonschema. However, to make it easier for people with a
more relational mind- or toolset it is possible to express amsterdam schema as a set of
relational tables. These tables are *meta_dataset*, *meta_table* and *meta_field*.

It is possible to convert a jsonschema into the relational table structure and vice-versa.

This command converts a dataset from an existing dataset in jsonschema format:

    schema import schema <id of dataset>

To convert from relational tables back to jsonschema:

    schema show schema <id of dataset>
