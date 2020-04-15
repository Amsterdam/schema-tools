import json

import click
import requests

import jsonschema
import ndjson
from amsterdam_schema.utils import schema_def_from_url
from shapely.geometry import shape
from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import SQLAlchemyError

from .db import (
    create_meta_table_data,
    create_meta_tables,
    create_rows,
    fetch_table_names,
    fetch_schema_from_relational_schema,
)
from schematools.introspect.db import introspect_db_schema
from schematools.introspect.geojson import introspect_geojson_files
from .utils import ParserError

DEFAULT_SCHEMA_URL = "https://schemas.data.amsterdam.nl/datasets/"
metadata = MetaData()

option_db_url = click.option(
    "--db-url",
    envvar="DATABASE_URL",
    required=True,
    help="DSN of database, can also use DATABASE_URL environment.",
)
option_schema_url = click.option(
    "--schema-url",
    envvar="SCHEMA_URL",
    default=DEFAULT_SCHEMA_URL,
    show_default=True,
    required=True,
    help="Url where valid amsterdam schema files are found. "
    "SCHEMA_URL can also be provided as environment variable.",
)
argument_schema_location = click.argument(
    "schema_location", metavar="(DATASET-ID | DATASET-FILENAME)",
)


def _get_engine(db_url):
    """Initialize the SQLAlchemy engine, and report click errors"""
    try:
        return create_engine(db_url)
    except SQLAlchemyError as e:
        raise click.BadParameter(str(e), param_hint="--db-url")


def fetch_rows(fh, srid):
    data = ndjson.load(fh)
    for row in data:
        row["geometry"] = f"SRID={srid};{shape(row['geometry']).wkt}"
        yield row


def main():
    """Main entry point.

    This catches relevant errors, so the user is not
    confronted with internal tracebacks.
    """
    try:
        schema()
    except (EnvironmentError, SQLAlchemyError, ParserError) as e:
        click.echo(f"{e.__class__.__name__}: {e}", err=True)
        exit(1)


@click.group()
def schema():
    """Command line utility to work with Amsterdam Schema files."""
    pass


@schema.group("import")
def import_():
    """Subcommand to import data"""
    pass


@schema.group()
def show():
    """Show existing metadata"""
    pass


@schema.group()
def introspect():
    """Subcommand to generate a schema."""
    pass


@schema.command()
@click.argument("json_schema_url")
@click.argument("schema_url")
def validate(json_schema_url, schema_url):
    """Validate a JSON file against a JSON schema."""
    response = requests.get(json_schema_url)
    response.raise_for_status()
    schema = response.json()

    response = requests.get(schema_url)
    response.raise_for_status()
    instance = response.json()

    try:
        jsonschema.validate(instance=instance, schema=schema)
    except (jsonschema.ValidationError, jsonschema.SchemaError) as e:
        click.echo(str(e), err=True)
        exit(1)


@show.command("tablenames")
@option_db_url
def show_tablenames(db_url):
    """Retrieve tablenames from a database."""
    engine = _get_engine(db_url)
    click.echo("\n".join(fetch_table_names(engine)))


@show.command("schema")
@option_db_url
@click.argument("dataset_id")
def show_schema(db_url, dataset_id):
    """Generate a schema based on an exising relational database."""
    engine = _get_engine(db_url)
    aschema = fetch_schema_from_relational_schema(engine, dataset_id)
    click.echo(json.dumps(aschema, indent=2))


@introspect.command("db")
@click.option("--prefix", "-p", help="Tables have prefix that needs to be stripped")
@option_db_url
@click.argument("dataset_id")
@click.argument("tables", nargs=-1)
def introspect_db(prefix, db_url, dataset_id, tables):
    """Generate a schema for the tables in a database"""
    engine = _get_engine(db_url)
    aschema = introspect_db_schema(engine, dataset_id, tables, prefix)
    click.echo(json.dumps(aschema, indent=2))


@introspect.command("geojson")
@click.argument("dataset_id")
@click.argument("files", nargs=-1, required=True)
def introspect_geojson(dataset_id, files):
    """Generate a schema from a GeoJSON file."""
    aschema = introspect_geojson_files(dataset_id, files)
    click.echo(json.dumps(aschema, indent=2))


@import_.command("ndjson")
@option_db_url
@option_schema_url
@argument_schema_location
@click.argument("table_name")
@click.argument("ndjson_path")
def import_ndjson(db_url, schema_url, schema_location, table_name, ndjson_path):
    """Import an NDJSON file into a table."""
    # Add batching for rows.
    engine = _get_engine(db_url)
    dataset_schema = schema_def_from_url(schema_url, schema_location)
    srid = dataset_schema["crs"].split(":")[-1]
    with open(ndjson_path) as fh:
        data = list(fetch_rows(fh, srid))
    create_rows(engine, metadata, dataset_schema, table_name, data)


@import_.command("schema")
@option_db_url
@option_schema_url
@argument_schema_location
def import_schema(db_url, schema_url, schema_location):
    """Import the schema definition into the local database."""
    # Add drop or not flag
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(schema_url, schema_location)

    create_meta_tables(engine)
    create_meta_table_data(engine, dataset_schema)


def _get_dataset_schema(schema_url, schema_location) -> DatasetSchema:
    """Find the dataset schema for the given dataset"""
    if "." in schema_location or "/" in schema_location:
        click.echo(f"Reading schema from {schema_location}")
        return DatasetSchema.from_file(schema_location)
    else:
        # Read the schema from the online repository.
        click.echo(f"Reading schemas from {schema_url}")
        try:
            return schema_def_from_url(schema_url, schema_location)
        except KeyError:
            raise click.BadParameter(f"Schema {schema_location} not found.")
