# amsterdam-schema-tools

Set of libraries and tools to work with Amsterdam schema.

Install the package with: `pip install amsterdam-schema-tools`. This installs
the library and a command-line tool called `schema`, with various subcommands.
A listing can be obtained from `schema --help`.

Subcommands that talk to a PostgreSQL database expect either a `DATABASE_URL`
environment variable or a command line option `--db-url` with a DSN.

Many subcommands want to know where to find schema files. Most will look in a
directory of schemas denoted by the `SCHEMA_URL` environment variable or the
`--schema-url` command line option. E.g.,

    schema create tables --schema-url=myschemas mydataset

will try to load the schema for `mydataset` from
`myschemas/mydataset/dataset.json`.


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
    rev: v3.5.0
    hooks:
      - id: validate-schema
        args: ['https://schemas.data.amsterdam.nl/schema@v1.2.0#']
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
Hence, we should not only bump version numbers on updates to this package,
but also commit a tag with the version number; see below.

## Doing a release

(This is for schema-tools developers.)

We use GitHub pull requests. If your PR should produce a new release of
schema-tools, make sure one of the commit increments the version number in
``setup.cfg`` appropriately. Then,

* merge the commit in GitHub, after review;
* pull the code from GitHub and merge it into the master branch,
  ``git checkout master && git fetch origin && git merge --ff-only origin/master``;
* tag the release X.Y.Z with ``git tag -a vX.Y.Z -m "Bump to vX.Y.Z"``;
* push the tag to GitHub with ``git push origin --tags``;
* release to PyPI: ``make upload`` (requires the PyPI secret).
