# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`

Currently, the following cli commands are available:

    - schema fetch tablenames
    - schema create schema <dataset-id> <list-of-tablenames>

The tools expect either a DATABASE_URL environment variable or a command-line option `--db-url` with a DSN.

The output is a json-schema output according to the Amsterdam schemas definition for the tables that are being processed.
