import json

import click
import requests

import jsonschema
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pg_grant import query
from .permissions import create_acl_from_profiles, introspect_permissions, revoke_permissions
from .permissions import create_acl_from_schema, create_acl_from_schemas, apply_schema_and_profile_permissions


from .db import (
    create_meta_table_data,
    create_meta_tables,
    fetch_table_names,
    fetch_schema_from_relational_schema,
)
from .exceptions import ParserError
from .introspect.db import introspect_db_schema
from .introspect.geojson import introspect_geojson_files
from .importer.geojson import GeoJSONImporter
from .importer.ndjson import NDJSONImporter
from .importer.base import BaseImporter
from .maps import create_mapfile
from .types import DatasetSchema, SchemaType
from .utils import schema_def_from_url, profile_defs_from_url, schema_fetch_url_file, schema_defs_from_url
from .provenance.create import ProvenaceIteration

DEFAULT_SCHEMA_URL = "https://schemas.data.amsterdam.nl/datasets/"
DEFAULT_PROFILE_URL = "https://schemas.data.amsterdam.nl/profiles/"


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

option_profile_url = click.option(
    "--profile-url",
    envvar="PROFILE_URL",
    default=DEFAULT_PROFILE_URL,
    show_default=True,
    required=True,
    help="Url where valid amsterdam profile files are found. "
    "PROFILE_URL can also be provided as environment variable.",
)

argument_profile_location = click.argument(
    "profile_location", metavar="(PROFILE-FILENAME | NONE)",
)


argument_role = click.argument(
    "role",
)

argument_scope = click.argument(
    "scope",
)

def _get_engine(db_url, pg_schemas=None):
    """Initialize the SQLAlchemy engine, and report click errors"""
    kwargs = {}
    if pg_schemas is not None:
        csearch_path = ",".join(pg_schemas + ["public"])
        kwargs["connect_args"] = {"options": f"-csearch_path={csearch_path}"}
    try:
        return create_engine(db_url, **kwargs)
    except SQLAlchemyError as e:
        raise click.BadParameter(str(e), param_hint="--db-url")


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
def permissions():
    """Show existing metadata"""
    pass


@permissions.command("introspect")
@option_db_url
@argument_role
def permissions_introspect(db_url, role):
    """Retrieve ACLs from a database."""
    engine = _get_engine(db_url)
    introspect_permissions(engine, role)


@permissions.command("revoke")
@option_db_url
@argument_role
def permissions_revoke(db_url, role):
    """Revoke all table select priviliges for role."""
    engine = _get_engine(db_url)
    revoke_permissions(engine, role)


@permissions.command("apply")
@option_db_url
@option_schema_url
@option_profile_url
@argument_schema_location
@argument_profile_location
@argument_role
@argument_scope
@click.option("--dry-run", is_flag=True, default=False, help="Don't execute the GRANT statements")
def permissions_apply(db_url, schema_url, profile_url, schema_location, profile_location, role, scope, dry_run):
    """Set permissions for a postgres role associated with a scope from Amsterdam Schema or Profiles."""
    def _fetch_json(location):
        if not location.startswith("http"):
            with open(location) as f:
                json_obj = json.load(f)
        else:
            response = requests.get(location)
            response.raise_for_status()
            json_obj = response.json()
        return json_obj


    engine = _get_engine(db_url)
    if schema_location in {"None", "NONE"}:
        ams_schema = None
    elif schema_location == 'ALL':
        ams_schema = schema_defs_from_url(schemas_url=schema_url)
    else:
        dataset_schema = _get_dataset_schema(schema_url, schema_location)
        ams_schema = {dataset_schema.id: dataset_schema}
    if profile_location in {"None", "NONE"}:
        profiles = None
    elif profile_location == 'ALL':
        profiles = profile_defs_from_url(schemas_url=profile_url)
    else:
        profile = _fetch_json(profile_location)
        profiles = {profile["name"]: profile}
    apply_schema_and_profile_permissions(engine, ams_schema, profiles, role, scope, dry_run)


@schema.group()
def introspect():
    """Subcommand to generate a schema."""
    pass

@schema.group()
def create():
    """Subcommand to create a DB object."""
    pass

@schema.command()
@click.argument("meta_schema_url")
@click.argument("schema_location")
def validate(meta_schema_url, schema_location):
    """ Validate a JSON file against the amsterdam schema meta schema.
    schema_location can be a url or a filesystem path.
    """

    def _fetch_json(location):
        if not location.startswith("http"):
            with open(location) as f:
                json_obj = json.load(f)
        else:
            response = requests.get(location)
            response.raise_for_status()
            json_obj = response.json()
        return json_obj

    schema = _fetch_json(meta_schema_url)
    instance = _fetch_json(schema_location)

    try:
        jsonschema.validate(instance=instance, schema=schema)
    except (jsonschema.ValidationError, jsonschema.SchemaError) as e:
        click.echo(str(e), err=True)
        exit(1)


@show.command("provenance")
@click.argument("schema_location")
def show_provenance(schema_location):
    """Retrieve the key-values pairs of the source column (specified as a 'provenance' property of an attribute) and its translated name (the attribute name itself)"""
    data = schema_fetch_url_file(schema_location)
    try:
        instance = ProvenaceIteration(data)
        click.echo(instance.final_dic)
    except (jsonschema.ValidationError, jsonschema.SchemaError, KeyError) as e:
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
    """Generate a json schema based on a schema define in a relational database."""
    engine = _get_engine(db_url, pg_schemas=["meta"])
    aschema = fetch_schema_from_relational_schema(engine, dataset_id)
    click.echo(json.dumps(aschema, indent=2))


@show.command("mapfile")
@click.option(
    "--schema-url",
    envvar="SCHEMA_URL",
    default=DEFAULT_SCHEMA_URL,
    show_default=True,
    required=True,
    help="Url where valid amsterdam schema files are found. "
    "SCHEMA_URL can also be provided as environment variable.",
)
@click.argument("dataset_id")
def show_mapfile(schema_url, dataset_id):
    """Generate a mapfile based on a dataset schema."""
    try:
        dataset_schema = schema_def_from_url(schema_url, dataset_id)
    except KeyError:
        raise click.BadParameter(f"Schema {dataset_id} not found.")
    click.echo(create_mapfile(dataset_schema))


@introspect.command("db")
@click.option(
    "--db-schema", "-s", help="Tables are in a different postgres schema (not 'public')"
)
@click.option("--prefix", "-p", help="Tables have prefix that needs to be stripped")
@option_db_url
@click.argument("dataset_id")
@click.argument("tables", nargs=-1)
def introspect_db(db_schema, prefix, db_url, dataset_id, tables):
    """Generate a schema for the tables in a database"""
    engine = _get_engine(db_url)
    aschema = introspect_db_schema(engine, dataset_id, tables, db_schema, prefix)
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
@click.option("--truncate-table", is_flag=True)
def import_ndjson(
    db_url, schema_url, schema_location, table_name, ndjson_path, truncate_table
):
    """Import a NDJSON file into a table."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(schema_url, schema_location)
    importer = NDJSONImporter(dataset_schema, engine)
    importer.load_file(ndjson_path, table_name, truncate=truncate_table)


@import_.command("geojson")
@option_db_url
@option_schema_url
@argument_schema_location
@click.argument("table_name")
@click.argument("geojson_path")
@click.option("--truncate-table", is_flag=True)
def import_geojson(
    db_url, schema_url, schema_location, table_name, geojson_path, truncate_table
):
    """Import a GeoJSON file into a table."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(schema_url, schema_location)
    importer = GeoJSONImporter(dataset_schema, engine)
    importer.load_file(geojson_path, table_name, truncate=truncate_table)


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


@create.command("identifierindex")
@option_db_url
@option_schema_url
def create_identifier_index(schema_url, db_url):
    """Execute SQLalchemy Index based on Identifier in the JSON schema data defintion"""
    data = schema_fetch_url_file(schema_url)
    engine = _get_engine(db_url)
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    importer = BaseImporter(dataset_schema, engine)

    for table in data['tables']:
        importer.generate_db_objects(table['id'], ind_tables=False)

@create.command("tables")
@option_db_url
@option_schema_url
def create_tables(schema_url, db_url):
    """Execute SQLalchemy Table objects"""
    data = schema_fetch_url_file(schema_url)
    engine = _get_engine(db_url)
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    importer = BaseImporter(dataset_schema, engine)

    for table in data['tables']:
        importer.generate_db_objects(table['id'], ind_identifier_index=False)

@create.command("all")
@option_db_url
@option_schema_url
def create_all_objects(schema_url, db_url):
    """Execute SQLalchemy Index (Identifier fields) and Table objects"""
    data = schema_fetch_url_file(schema_url)
    engine = _get_engine(db_url)
    parent_schema = SchemaType(data)
    dataset_schema = DatasetSchema(parent_schema)
    importer = BaseImporter(dataset_schema, engine)

    for table in data['tables']:
        importer.generate_db_objects(table['id'])