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

The schematools library has a module that reads GOB events into database tables that are
defines by an Amsterdam schema. This module can be used to read GOB events from a Kafka stream.
It is also possible to read GOB events from a batch file with line-separeted events using:

    schema import events <path-to-dataset> <path-to-file-with-events>


## Export datasets

Datasets can be exported to different file formats. Currently supported are geopackage,
csv and jsonlines. The command for exporting the dataset tables is:

    schema export [geopackage|csv|jsonlines] <id of dataset>

The command has several command-line options that can be used. Documentations about these
flags can be shown using the `--help` options.


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


## Mocking data

The schematools library contains two Django management commands to generate
mock data. The first one is `create_mock_data` which generates mock data for all
the datasets that are found at the configured schema location `SCHEMA_URL`
(where `SCHEMA_URL` can be configured to point to a path at the local filesystem).

The `create_mock_data` command expects either a list of dataset ids to include or a
list of dataset ids to exclude. The datasets to include can be provided as positional arguments
or using the --datasets-list argument, which defaults to the environment variable
`DATASETS_LIST`. To exclude datasets the `--datasets-exclude` argument or the
environment variables `DATASET_EXCLUDE` can be used.

Furthermore, the command has the options to change the default number of
generated records (`--size`).

To avoid duplicate primary keys on subsequent runs the `--start-at` options can be used
to start autonumbering of primary keys at an offset.

E.g. to generate 5 records for the `bag` and `gebieden` datasets, starting the
autonumbering of primary keys at 50.

```
    django create_mock_data bag gebieden --size 5 --start-at 50
```

or by using the environment variable

```
    export DATASETS_LIST=bag,gebieden
    django create_mock_data --size 5 --start-at 50
```

To generate records for all datasets, except for the `fietspaaltjes` dataset:

```
    django create_mock_data --datasets-exclude fietspaaltjes  # or --exclude
```

To generate records for the `bbga` dataset, by loading the schema from the local filesystem:

```
    django create_mock_data <path-to-bbga-schema>/datasets.json
```

During record generation in `create_mock_data`, the relations are not added,
so foreign key fields will be filled with NULL values.

There is a second management command `relate_mock_data` that can be used to
add the relations. This command support positional arguments for datasets
in the same way as `create_mock_data`.
Furthermore, the command also has the `--exclude` option to reverse the meaning
of the positional dataset arguments.

E.g. to add relations to all datasets:

```
    django relate_mock_data
```

To add relations for `bag` and `gebieden` only:

```
    django relate_mock_data bag gebieden
```

To add relations for all datasets except `meetbouten`:

```
    django relate_mock_data --datasets-exclude meetbouten # or --exclude
```

NB. When only a subset of the datasets is being mocked, the command can fail when datasets that
are involved in a relation are missing, so make sure to include all relevant
datasets.

For convenience an additional management command `truncate_tables` has been added,
to truncate all tables.
