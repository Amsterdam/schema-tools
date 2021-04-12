# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`

Currently, the following cli commands are available:

- schema import events
- schema import ndjson
- schema show schema <dataset-id>
- schema show tablenames
- schema introspect db <dataset-id> <list-of-tablenames>
- schema introspect geojson <dataset-id> \*.geojson
- schema validate
- schema permissions apply

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


## Generating amsterdam schema from existing GeoJSON files

The following command can be used to inspect and import the GeoJSON files:

    schema introspect geojson <dataset-id> *.geojson > schema.json
    edit schema.json  # fine-tune the table names
    schema import geojson schema.json <table1> file1.geojson
    schema import geojson schema.json <table2> file2.geojson

## Importing GOB events

The schematools library has a module that read GOB events into database tables that are
defines by an Amsterdam schema. This module can be used to read GOB events from a Kafka stream.
It is also possible to read GOB events from a batch file with line-separeted events using:

    schema import events <path-to-dataset> <path-to-file-with-events>

## Schema Tools as a pre-commit hook

Included in the project is a `pre-commit` hook
that can validate schema files
in a project such as [amsterdam-schema](https://github.com/Amsterdam/amsterdam-schema)

To configure it
extend the `.pre-commit-config.yaml`
in the project with the schema file defintions as follows:

```yaml
  - repo: https://github.com/Amsterdam/schema-tools
    rev: v0.20.2
    hooks:
      - id: validate-schema
        args: ['https://schemas.data.amsterdam.nl/schema@v1.1.1#']
        exclude: |
            (?x)^(
                schema.+|             # exclude meta schemas
                datasets/index.json
            )$
```

`args` is a one element list
containing the URL to the Amsterdam Meta Schema.

`validate-schema` will only process `json` files.
However not all `json` files are Amsterdam schema files.
To exclude files or directories use `exclude` with pattern.

`pre-commit` depends on properly tagged revisions of its hooks.
Hence we should take care to, not only bump version numbers
on updates to this package,
but also commit a tag with the version number.
This is automated by means of the `tbump` tool.
Bumping a version from 0.18.1 to 0.18.2
and generating the appropriate git commits/tags
is as easy as running:

```console
$ tbump 0.18.2
```
