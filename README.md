# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`

Currently, the following cli commands are available:

    - schema fetch tablenames
    - schema create schema <dataset-id> <list-of-tablenames>

The tools expect either a DATABASE_URL environment variable or a command-line option `--db-url` with a DSN.

The output is a json-schema output according to the Amsterdam schemas definition for the tables that are being processed.

The --prefix argument controls whether table prefixes are removed in the schema, because that is required for Django
models.

As example we can generate a BAG schema. Point DATABASE_URL to bag_v11 database and then run :

    schema fetch tablenames | awk '/^bag_/{print}' | xargs schema create schema bag --prefix bag_ | jq

The **jq** formats it nicely and it can be redirected to the correct directory in the schemas repository directly.
