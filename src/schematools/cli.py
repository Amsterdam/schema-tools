"""Cli tools."""
from __future__ import annotations

import io
import json
import logging
import operator
import os
import sys
from collections import defaultdict
from functools import reduce
from importlib.metadata import version
from pathlib import Path, PosixPath
from typing import Any, DefaultDict, Iterable, List

import click
import jsonschema
import requests
import sqlalchemy
from deepdiff import DeepDiff
from jsonschema import draft7_format_checker
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import create_engine
from sqlalchemy.schema import CreateTable

from schematools import (
    COMPATIBLE_METASCHEMAS,
    DEFAULT_PROFILE_URL,
    DEFAULT_SCHEMA_URL,
    ckan,
    validation,
)
from schematools.events.full import EventsProcessor
from schematools.exceptions import (
    DatasetNotFound,
    IncompatibleMetaschema,
    ParserError,
    SchemaObjectNotFound,
)
from schematools.exports.csv import export_csvs
from schematools.exports.geopackage import export_geopackages
from schematools.exports.jsonlines import export_jsonls
from schematools.factories import tables_factory
from schematools.importer.base import BaseImporter
from schematools.importer.geojson import GeoJSONImporter
from schematools.importer.ndjson import NDJSONImporter
from schematools.introspect.db import introspect_db_schema
from schematools.introspect.geojson import introspect_geojson_files
from schematools.loaders import FileSystemSchemaLoader, get_schema_loader
from schematools.maps import create_mapfile
from schematools.naming import to_snake_case, toCamelCase
from schematools.permissions.db import (
    apply_schema_and_profile_permissions,
    introspect_permissions,
    revoke_permissions,
)
from schematools.provenance.create import ProvenanceIteration
from schematools.types import DatasetSchema, DatasetTableSchema, Publisher, SemVer

# Configure a simple stdout logger for permissions output
logger = logging.getLogger("schematools.permissions")
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
# Add simple formatting for cli useage
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
pkg_version = version("amsterdam-schema-tools")

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

argument_dataset_id = click.argument(
    "dataset_id",
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
    "profile_location",
    metavar="(PROFILE-FILENAME | NONE)",
)

argument_role = click.argument(
    "role",
)


def _get_engine(db_url: str, pg_schemas: list[str] | None = None) -> sqlalchemy.engine.Engine:
    """Initialize the SQLAlchemy engine, and report click errors."""
    kwargs = {}
    if pg_schemas is not None:
        csearch_path = ",".join(pg_schemas + ["public"])
        kwargs["connect_args"] = {"options": f"-csearch_path={csearch_path}"}
    try:
        return create_engine(db_url, **kwargs)
    except SQLAlchemyError as e:
        raise click.BadParameter(str(e), param_hint="--db-url") from e


def main() -> None:
    """Main entry point.

    This catches relevant errors, so the user is not
    confronted with internal tracebacks.
    """
    try:
        schema()
    except (OSError, SQLAlchemyError, ParserError) as e:
        click.echo(f"{e.__class__.__name__}: {e}", err=True)
        exit(1)


@click.group()
def schema() -> None:
    """Command line utility to work with Amsterdam Schema files."""
    pass


@schema.group("import")
def import_() -> None:
    """Subcommand to import data."""
    pass


@schema.group("export")
def export() -> None:
    """Subcommand to export data."""
    pass


@schema.group("tocase")
def tocase() -> None:
    """Subcommand to make case-changes."""
    pass


@schema.group()
def show() -> None:
    """Show existing metadata."""
    pass


@schema.group()
def permissions() -> None:
    """Subcommand for permissions."""
    pass


@schema.group()
def kafka() -> None:
    """Subcommand to consume or produce kafka events."""
    pass


@permissions.command("introspect")
@option_db_url
@argument_role
def permissions_introspect(db_url: str, role: str) -> None:
    """Retrieve ACLs from a database."""
    engine = _get_engine(db_url)
    introspect_permissions(engine, role)


@permissions.command("revoke")
@option_db_url
@argument_role
@click.option("-v", "--verbose", count=True)
def permissions_revoke(db_url: str, role: str, verbose: int) -> None:
    """Revoke all table select priviliges for role."""
    engine = _get_engine(db_url)
    revoke_permissions(engine, role, verbose=verbose)


@permissions.command("apply")
@option_db_url
@option_schema_url
@option_profile_url
@click.option(
    "--schema-filename",
    is_flag=False,
    help="Filename of local Amsterdam Schema (single dataset)."
    " If specified, it will be used instead of schema-url",
)
@click.option(
    "--profile-filename",
    is_flag=False,
    help="Filename of local Profile. If specified, it will be used instead of profile-url",
)
@click.option(
    "--pg_schema",
    is_flag=False,
    default="public",
    show_default=True,
    help="Postgres schema containing the data",
)
@click.option(
    "--auto",
    is_flag=True,
    default=False,
    help="Grant each scope X to their associated db role scope_x.",
)
@click.option(
    "--role",
    is_flag=False,
    default="",
    help="Role to receive grants. Ignored when --auto=True",
)
@click.option(
    "--scope",
    is_flag=False,
    default="",
    help="Scope to be granted. Ignored when --auto=True",
)
@click.option(
    "--execute/--dry-run",
    default=False,
    help="Execute SQL statements or dry-run [default]",
)
@click.option(
    "--read/--no-read",
    "set_read_permissions",
    default=True,
    help="Set read permissions [default=read]",
)
@click.option(
    "--write/--no-write",
    "set_write_permissions",
    default=True,
    help="Set dataset-level write permissions [default=write]",
)
@click.option("--create-roles", is_flag=True, default=False, help="Create missing postgres roles")
@click.option(
    "--revoke",
    is_flag=True,
    default=False,
    help="Before granting new permissions, revoke first all previous table and column permissions",
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "-a",
    "--additional-grants",
    multiple=True,
    help="""Additional grants can be defined in the following format:
            `<table_name>:<privilege_1>[,<privilege_n>]*;<grantee_1>[,grantee_n]*`
            Add one option for every table.
            NB: Surround values with double quotes!
              """,
)
def permissions_apply(
    db_url: str,
    schema_url: str,
    profile_url: str,
    schema_filename: str,
    profile_filename: str,
    pg_schema: str,
    auto: bool,
    role: str,
    scope: str,
    execute: bool,
    create_roles: bool,
    set_read_permissions: bool,
    set_write_permissions: bool,
    revoke: bool,
    verbose: int,
    additional_grants: tuple[str] = (),
) -> None:
    """Set permissions for a postgres role.

    This is based on a scope from Amsterdam Schema or Profiles.
    """
    dry_run = not execute

    if auto:
        role = "AUTO"
        scope = "ALL"

    engine = _get_engine(db_url)

    if schema_filename:
        loader = FileSystemSchemaLoader.from_file(schema_filename)
        dataset_schema = loader.get_dataset_from_file(schema_filename)
        ams_schema = {dataset_schema.id: dataset_schema}
    else:
        ams_schema = get_schema_loader(schema_url).get_all_datasets()

    if profile_filename:
        profile = _schema_fetch_url_file(profile_filename)
        profiles = {profile["name"]: profile}
    else:
        # Profiles not live yet, temporarilly commented out
        # profiles = profile_defs_from_url(profiles_url=profile_url)
        profiles = None

    if auto or (role and scope):
        apply_schema_and_profile_permissions(
            engine,
            pg_schema,
            ams_schema,
            profiles,
            role,
            scope,
            set_read_permissions,
            set_write_permissions,
            dry_run,
            create_roles,
            revoke,
            verbose=verbose,
            additional_grants=additional_grants,
        )
    else:
        click.echo(
            "Choose --auto or specify both a --role and a --scope to be able to grant permissions"
        )


def _schema_fetch_url_file(schema_url_file: str) -> dict[str, Any]:
    """Return schemadata from URL or File."""
    # XXX Does not work with datasets that have their tables split
    # out into separate files. Should use _get_dataset_schema instead.

    if not schema_url_file.startswith("http"):
        with open(schema_url_file) as f:
            schema_data = json.load(f)
    else:
        response = requests.get(schema_url_file)
        response.raise_for_status()
        schema_data = response.json()

    return schema_data


@schema.group()
def introspect() -> None:
    """Subcommand to generate a schema."""
    pass


@schema.group()
def create() -> None:
    """Subcommand to create a DB object."""
    pass


@schema.group()
def diff() -> None:
    """Subcommand to show schema diffs."""
    pass


def _fetch_json(location: str) -> dict[str, Any]:
    """Fetch JSON from file or URL.

    Args:
        location: a file name or an URL

    Returns:
        JSON data as a dictionary.
    """
    if not location.startswith("http"):
        with open(location) as f:
            json_obj = json.load(f)
    else:
        response = requests.get(location)
        response.raise_for_status()
        json_obj = response.json()
    return json_obj


@schema.command()
@option_schema_url
@argument_dataset_id
@click.option(
    "--additional-schemas",
    "-a",
    multiple=True,
    help=(
        "Id of a dataset schema that will be preloaded. "
        "To be used mainly for schemas that are related to the schema that is being validated."
    ),
)
@click.argument("meta_schema_url", nargs=-1)
def validate(
    schema_url: str, dataset_id: str, additional_schemas: list[str], meta_schema_url: tuple[str]
) -> None:
    """Validate a schema against the Amsterdam Schema meta schema.

    Args:

    \b
        DATASET_ID: id of the dataset.
        META_SCHEMA_URL: URL where the meta schema for Amsterdam Schema definitions can be found.
        If multiple are given, schematools will try to validate against the largest version,
        working backwards and stopping at the first version that the objects are valid against.

        Usually META_SCHEMA_URL is something like: https://schemas.data.amsterdam.nl/schema@vn
    """  # noqa: D301,D412,D417
    if not meta_schema_url:
        click.echo("META_SCHEMA_URL not provided", err=True)
        sys.exit(1)

    dataset = _get_dataset_schema(dataset_id, schema_url, prefetch_related=True)

    # The additional schemas are fetched, but the result is not used
    # because the only reason to fetch the additional schemas is to have those schemas
    # available in the cache that is part of the DatasetSchema class
    for schema in additional_schemas:
        _get_dataset_schema(schema, schema_url)

    exit_status = 0

    for meta_schema_version, url in sorted(
        [(version_from_metaschema_url(u), u) for u in set(meta_schema_url)],
        reverse=True,
    ):
        click.echo(f"Validating against metaschema {meta_schema_version}")
        meta_schema = _fetch_json(url)
        if meta_schema_version.major not in COMPATIBLE_METASCHEMAS:
            raise IncompatibleMetaschema(
                f"Schematools {pkg_version} is not compatible"
                f" with metaschema {meta_schema_version}"
            )
        structural_errors = False

        try:
            jsonschema.validate(
                instance=dataset.json_data(inline_tables=True, inline_publishers=False),
                schema=meta_schema,
                format_checker=draft7_format_checker,
            )
        except (jsonschema.ValidationError, jsonschema.SchemaError) as e:
            click.echo("Structural validation: ", nl=False)
            structural_errors = True
            click.echo(format_schema_error(e), err=True)

        semantic_errors = False
        for error in validation.run(dataset):
            if not semantic_errors:  # Only print on first error.
                click.echo("Semantic validation: ", nl=False)
                semantic_errors = True
            click.echo(f"\n{error!s}", err=True)

        if structural_errors or semantic_errors:
            click.echo(f"Dataset is invalid against {meta_schema_version}")
            exit_status = 1
        else:
            click.echo(f"Dataset is valid against {meta_schema_version}")

    sys.exit(exit_status)


def version_from_metaschema_url(url: str) -> SemVer:  # noqa: D103
    return SemVer(url.rpartition("@")[2])


@schema.command()
@option_schema_url
@click.argument("meta_schema_url", nargs=-1)
def validate_publishers(schema_url: str, meta_schema_url: tuple[str]) -> None:
    """Validate all publishers against the Amsterdam Schema meta schema.

    Args:

    \b
        META_SCHEMA_URL: URL where the meta schema for Amsterdam Schema definitions can be found.
        If multiple are given, schematools will try to validate against the largest version,
        working backwards and stopping at the first version that the objects are valid against.

    Options:

    \b
        SCHEMA_URL: URL where the datasets for Amsterdam Schema definitions can be found. The path
        component of this uri is dropped to find the publishers in the root. For example, if
        SCHEMA_URL=https://example.com/datasets, the publishers are extracted from
        https://example.com/publishers.
    """  # noqa: D301,D412,D417
    for meta_schema_version, url in sorted(
        [(version_from_metaschema_url(u), u) for u in set(meta_schema_url)],
        reverse=True,
    ):
        meta_schema = _fetch_json(url)
        if meta_schema_version.major not in COMPATIBLE_METASCHEMAS:
            raise IncompatibleMetaschema(
                f"Schematools {pkg_version} is not"
                f"compatible with metaschema {meta_schema_version}"
            )

        click.echo(f"Validating against metaschema {meta_schema_version}")
        publishers = _get_publishers(schema_url)

        structural_errors = False
        for id_, publisher in publishers.items():
            try:
                click.echo(f"Validating publisher with id {id_}")
                jsonschema.validate(
                    instance=publisher.json_data(),
                    schema=meta_schema,
                    format_checker=draft7_format_checker,
                )
            except (jsonschema.ValidationError, jsonschema.SchemaError) as e:
                click.echo("Structural validation: ", nl=False)
                structural_errors = True
                click.echo(format_schema_error(e), err=True)

        if structural_errors:
            continue
        click.echo(f"All publishers are structurally valid against {meta_schema_version}")
        sys.exit(0)
    click.echo("Publishers are structurally invalid against all supplied metaschema versions")
    sys.exit(1)


@schema.command()
@click.argument("meta_schema_url")
@click.argument("schema_files", nargs=-1)
@click.option(
    "-m",
    "--extra_meta_schema_url",
    help="An additional metaschema to try validation in case meta_schema_url fails",
)
def batch_validate(
    meta_schema_url: str, schema_files: tuple[str], extra_meta_schema_url: str
) -> None:
    """Batch validate schemas.

    This command was tailored so that it could be run from a pre-commit hook.
    The order and type of its arguments differ from other `schema` sub-commands:
    the files it accepts are either "dataset.json" files or files denoting
    tables. In the case of table files, the containing dataset will be
    validated.

    It will perform both structural and semantic validation of schemas.
    If extra_meta_schema_url is supplied, meta_schema_url will be tried first.

    Args:

    \b
        META_SCHEMA_URL: the URL to the Amsterdam meta schema
        SCHEMA_FILES: one or more schema files to be validated
    """  # noqa: D301,D412,D417
    errors: DefaultDict[str, defaultdict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    # Find the root "datasets" directory.
    datasets_dir = Path(os.path.commonpath(schema_files)).absolute().resolve()
    path_parts = datasets_dir.parts

    # Bail out if there is no `datasets` directory
    try:
        datasets_idx = path_parts.index("datasets")
    except ValueError:
        raise ValueError("dataset files do not live in a common 'datasets' dir")

    # Find out if we need to go up the directory tree to get at `datasets` dir
    # This could be needed if we are only checking one dataset,
    # because in that case datasets_dir is initially the full path to
    # the `dataset.json` file.
    up_tree_count = len(path_parts) - 2 - datasets_idx
    if up_tree_count > 0:
        datasets_dir = datasets_dir.parents[up_tree_count]

    loader = FileSystemSchemaLoader(datasets_dir)

    done = set()
    for schema_file in schema_files:
        # If the schema file is a table, find the dataset.json
        # file in one of the parent directories.
        ds_dir = Path(schema_file).parent
        while not (ds_dir / "dataset.json").exists():
            ds_dir = ds_dir.parent

        # Don't run validations multiple times when several files in a dataset have changed.
        main_file = os.path.join(ds_dir, "dataset.json")
        if main_file in done:
            continue

        meta_schema_urls = [meta_schema_url]
        if extra_meta_schema_url:
            meta_schema_urls.append(extra_meta_schema_url)
        for url in meta_schema_urls:
            meta_schema_version = version_from_metaschema_url(url)
            click.echo(f"Validating {main_file} against {meta_schema_version}")

            try:
                dataset = loader.get_dataset_from_file(main_file)
            except ValueError as ve:
                errors[schema_file][meta_schema_version].append(str(ve))
                # No sense in continuing if we can't read the schema file.
                break

            meta_schema = _fetch_json(url)
            try:
                jsonschema.validate(
                    instance=dataset.json_data(inline_tables=True, inline_publishers=False),
                    schema=meta_schema,
                    format_checker=draft7_format_checker,
                )
            except (jsonschema.ValidationError, jsonschema.SchemaError) as struct_error:
                errors[schema_file][meta_schema_version].append(format_schema_error(struct_error))

            for sem_error in validation.run(dataset, main_file):
                errors[schema_file][meta_schema_version].append(str(sem_error))

            if not errors[schema_file][meta_schema_version]:
                click.echo(f"{schema_file} is valid against meta schema {meta_schema_version}")
                # We dont show errors if the file is valid against one of the metaschemas
                errors.pop(schema_file)
                break
        done.add(main_file)

    if errors:
        width = len(max(errors.keys(), key=lambda x: len(x)))
        for schema_file, versions in errors.items():
            click.echo(f"{schema_file} is invalid against all metaschema versions")
            version_width = len(max(versions.keys(), key=lambda x: len(x)))
            for version_, err_msgs in versions.items():
                for msg in err_msgs:
                    click.echo(f"{schema_file:>{width}} - {version_:>{version_width}}: {msg}")
        sys.exit(1)


def format_schema_error(e: jsonschema.SchemaError | jsonschema.ValidationError) -> str:
    s = io.StringIO()
    s.write(f"{e.json_path}, {list(e.schema_path)}")
    if e.message and not e.context:
        s.write("\n\t" + e.message)
    if e.context:
        for ec in e.context:
            s.write("\n\t" + ec.message.strip())
    return s.getvalue()


@schema.command("ckan")
@option_schema_url
@click.option(
    "--upload-url",
    "-u",
    default=None,
    help="URL for uploading, e.g., https://data.overheid.nl/data/ (none to print to stdout)",
)
def to_ckan(schema_url: str, upload_url: str):
    """Convert all schemas to CKAN format, and optionally upload them.

    The API key for CKAN is taken from the environment variable CKAN_API_KEY.
    """
    api_key = os.getenv("CKAN_API_KEY")
    if upload_url is not None and api_key is None:
        click.echo("CKAN_API_KEY not set in environment", err=True)
        exit(1)

    status = 0

    datasets = get_schema_loader(schema_url).get_all_datasets()

    data = []
    for path, ds in datasets.items():
        if ds.status != DatasetSchema.Status.beschikbaar:
            continue
        try:
            data.append(ckan.from_dataset(ds, path))
        except Exception as e:
            logger.error("in dataset %s: %s", ds.identifier, str(e))  # noqa: G200
            status = 1

    if upload_url is None:
        for ds in data:
            click.echo(ds)
        exit(0)

    headers = {"Authorization": api_key}
    for ds in data:
        ident = ds["identifier"]
        url = f"{upload_url}/api/3/action/package_update?id={ident}"
        response = requests.post(url, headers=headers, json=ds)
        logger.debug("%s: %d, %s", url, response.status_code, response.json())

        if response.status_code == 404:
            # 404 *can* mean no such dataset. Try again with package_create.
            url = upload_url + "/api/3/action/package_create"
            response = requests.post(url, headers=headers, json=ds)
            logger.debug("%s: %d, %s", url, response.status_code, response.json())

        if not (200 <= response.status_code < 300):
            logger.error("uploading %s: %s", ident, response.json())
            status = 1

    exit(status)


@show.command("provenance")
@click.argument("dataset_id")
def show_provenance(dataset_id: str, schema_url: str) -> None:
    """Retrieve the key-values pairs of the source column.

    (specified as a 'provenance' property of an attribute)
    and its translated name (the attribute name itself)
    """
    dataset = _get_dataset_schema(dataset_id, schema_url, prefetch_related=True)
    try:
        instance = ProvenanceIteration(dataset)
        click.echo(instance.final_dic)
    except (jsonschema.ValidationError, jsonschema.SchemaError, KeyError) as e:
        click.echo(str(e), err=True)
        exit(1)


@show.command("tablenames")
@option_db_url
def show_tablenames(db_url: str) -> None:
    """Retrieve tablenames from a database."""
    engine = _get_engine(db_url)
    names = inspect(engine).get_table_names()
    click.echo("\n".join(names))


@show.command("datasets")
@option_schema_url
@click.option("--to-snake-case", "snake_it", is_flag=True)
def show_datasets(schema_url: str, snake_it: bool) -> None:
    """Retrieve the ids of all the datasets."""
    loader = get_schema_loader(schema_url)
    modifier = to_snake_case if snake_it else lambda x: x
    for dataset_schema in loader.get_all_datasets().values():
        click.echo(modifier(dataset_schema.id))


@show.command("datasettables")
@option_schema_url
@argument_dataset_id
@click.option("--to-snake-case", "snake_it", is_flag=True)
def show_datasettables(schema_url: str, dataset_id: str, snake_it: bool) -> None:
    """Retrieve the ids of the datasettables for the indicated dataset."""
    dataset_schema = _get_dataset_schema(dataset_id, schema_url, prefetch_related=False)
    modifier = to_snake_case if snake_it else lambda x: x
    for dataset_table in dataset_schema.tables:
        click.echo(modifier(dataset_table.id))


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
def show_mapfile(schema_url: str, dataset_id: str) -> None:
    """Generate a mapfile based on a dataset schema."""
    try:
        dataset_schema = get_schema_loader(schema_url).get_dataset(dataset_id)
    except KeyError:
        raise click.BadParameter(f"Schema {dataset_id} not found.") from None
    click.echo(create_mapfile(dataset_schema))


@show.command("scopes")
@option_schema_url
@argument_dataset_id
def show_scopes(schema_url: str, dataset_id: str) -> None:
    """Generate a list of all the scopes used in the indicated dataset."""
    # We need to `unfreeze` the sets, to make the set operations work.
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    scopes = set(dataset_schema.auth)
    for table in dataset_schema.tables:
        scopes |= set(table.auth) | reduce(operator.or_, (set(f.auth) for f in table.fields))
    click.echo(" ".join(str(scope) for scope in scopes - {"OPENBAAR"}))


@introspect.command("db")
@click.option("--db-schema", "-s", help="Tables are in a different postgres schema (not 'public')")
@click.option("--prefix", "-p", help="Tables have prefix that needs to be stripped")
@option_db_url
@click.argument("dataset_id")
@click.argument("tables", nargs=-1)
def introspect_db(
    db_schema: str, prefix: str, db_url: str, dataset_id: str, tables: Iterable[str]
) -> None:
    """Generate a schema for the tables in a database."""
    engine = _get_engine(db_url)
    aschema = introspect_db_schema(engine, dataset_id, tables, db_schema, prefix)
    click.echo(json.dumps(aschema, indent=2))


@introspect.command("geojson")
@click.argument("dataset_id")
@click.argument("files", nargs=-1, required=True)
def introspect_geojson(dataset_id: str, files: List[str]) -> None:
    """Generate a schema from a GeoJSON file."""
    aschema = introspect_geojson_files(dataset_id, files)
    click.echo(json.dumps(aschema, indent=2))


@import_.command("ndjson")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.argument("table_id")
@click.argument("ndjson_path")
@click.option("--batch_size", default=100, type=int)
@click.option("--truncate-table", is_flag=True)
def import_ndjson(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    table_id: str,
    batch_size: int,
    ndjson_path: PosixPath,
    truncate_table: bool,
) -> None:
    """Import a NDJSON file into a table."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    importer = NDJSONImporter(dataset_schema, engine)
    importer.generate_db_objects(table_id, truncate=False, ind_tables=True, ind_extra_index=False)
    importer.load_file(ndjson_path, batch_size, truncate=truncate_table)


@import_.command("geojson")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.argument("table_id")
@click.argument("geojson_path")
@click.option("--batch_size", default=100, type=int)
@click.option("--truncate-table", is_flag=True)
def import_geojson(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    table_id: str,
    batch_size: int,
    geojson_path: PosixPath,
    truncate_table: bool,
) -> None:
    """Import a GeoJSON file into a table."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    importer = GeoJSONImporter(dataset_schema, engine)
    importer.generate_db_objects(table_id, truncate=False, ind_tables=True, ind_extra_index=False)
    importer.load_file(geojson_path, batch_size=batch_size, truncate=truncate_table)


@import_.command("events")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.option("--additional-schemas", "-a", multiple=True)
@click.argument("events_path")
@click.option("-t", "--truncate-table", default=False, is_flag=True)
def import_events(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    additional_schemas: str,
    events_path: str,
    truncate_table: bool,
) -> None:
    """Import an events file into a table."""
    engine = _get_engine(db_url)
    dataset_schemas = [_get_dataset_schema(dataset_id, schema_url)]
    for schema in additional_schemas:
        dataset_schemas.append(_get_dataset_schema(schema, schema_url))
    # Create connection, do not start a transaction.
    with engine.connect() as connection:
        importer = EventsProcessor(dataset_schemas, connection, truncate=truncate_table)
        importer.load_events_from_file(events_path)


def _get_dataset_schema(
    dataset_id: str, schema_url: str, prefetch_related: bool = False
) -> DatasetSchema:
    """Find the dataset schema for the given dataset.

    Args:
        dataset_id: id of the dataset.
        schema_url: url of the location where the collection of amsterdam schemas is found.
        prefetch_related: related schemas should be prefetched.
    """
    loader = get_schema_loader(schema_url)
    try:
        return loader.get_dataset(dataset_id, prefetch_related=prefetch_related)
    except DatasetNotFound as e:
        raise click.ClickException(str(e)) from None


def _get_publishers(schema_url: str) -> dict[str, Publisher]:
    """Find the publishers from the given schema_url.

    Args:
        dataset_id: id of the dataset.
        schema_url: url of the location where the collection of amsterdam schemas is found.
        prefetch_related: related schemas should be prefetched.
    """
    loader = get_schema_loader(schema_url)
    try:
        return loader.get_all_publishers()
    except SchemaObjectNotFound as e:
        raise click.ClickException(str(e)) from None


@create.command("extra_index")
@option_db_url
@option_schema_url
@argument_dataset_id
def create_identifier_index(db_url: str, schema_url: str, dataset_id: str) -> None:
    """Execute SQLalchemy Index based on Identifier in the JSON schema data definition."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    importer = BaseImporter(dataset_schema, engine)

    for table in dataset_schema.get_tables():
        importer.generate_db_objects(
            table.id,
            ind_tables=False,
            ind_extra_index=True,
            is_versioned_dataset=importer.is_versioned_dataset,
        )


@create.command("tables")
@option_db_url
@option_schema_url
@argument_dataset_id
def create_tables(db_url: str, schema_url: str, dataset_id: str) -> None:
    """Execute SQLalchemy Table objects."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url, prefetch_related=True)
    importer = BaseImporter(dataset_schema, engine)

    for table in dataset_schema.get_tables():
        importer.generate_db_objects(
            table.id,
            ind_extra_index=False,
            ind_tables=True,
            is_versioned_dataset=importer.is_versioned_dataset,
        )

@create.command("sql")
@click.option("--versioned/--no-versioned", default=True)
@option_db_url
@click.argument("schema_path")
def create_sql(versioned: bool, db_url: str, schema_path: str) -> None:
    """Generate SQL Create from amsterdam schema definition."""
    engine = _get_engine(db_url)
    loader = FileSystemSchemaLoader.from_file(schema_path)
    dataset_schema = loader.get_dataset_from_file(schema_path)
    tables = tables_factory(dataset_schema, is_versioned_dataset=versioned)
    for table in tables.values():
        table_sql = CreateTable(table).compile(engine)
        click.echo(str(table_sql))


@create.command("all")
@option_db_url
@option_schema_url
@click.option(
    "-x",
    "--exclude",
    multiple=True,
    type=str,
    default=[],
    help="dataset_id to exclude. Can be repeated.",
)
@click.argument("dataset_id", required=False)
def create_all_objects(
    db_url: str, schema_url: str, exclude: list[str], dataset_id: str | None
) -> None:
    """Execute SQLalchemy Index (Identifier fields) and Table objects.

    If no DATASET_ID is provide it will process all datasets!
    """
    loader = get_schema_loader(schema_url)
    if dataset_id is None:
        click.echo("No 'dataset_id' provided. Processing all datasets!")
        dataset_schemas = loader.get_all_datasets().values()
    else:
        dataset_schemas = [loader.get_dataset(dataset_id, prefetch_related=True)]

    engine = _get_engine(db_url)
    for dataset_schema in dataset_schemas:
        if dataset_schema.id in exclude:
            msg = f"Skipping dataset {dataset_id!r}"
            click.echo(msg)
            click.echo("=" * len(msg))
            continue
        msg = f"Processing dataset {dataset_id!r}"
        click.echo(msg)
        click.echo("-" * len(msg))
        importer = BaseImporter(dataset_schema, engine)
        for table in dataset_schema.get_tables():
            importer.generate_db_objects(
                table.id,
                is_versioned_dataset=importer.is_versioned_dataset,
            )


@diff.command("all")
@option_schema_url
@click.argument("diff_schema_url")
def diff_schemas(schema_url: str, diff_schema_url: str) -> None:
    """Show diff for two sets of schemas.

    The left-side schemas location is
    defined in SCHEMA_URL (or via --schema-url), the right-side schemas location
    has to be on the command-line.

    This can be used to compare two sets of schemas, e.g. ACC and PRD schemas.

    For nicer output, pipe it through a json formatter.
    """
    schemas = get_schema_loader(schema_url).get_all_datasets()
    diff_schemas = get_schema_loader(diff_schema_url).get_all_datasets()
    click.echo(DeepDiff(schemas, diff_schemas, ignore_order=True).to_json())


@export.command("geopackage")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.option("--output", "-o", default="/tmp")  # noqa: S108  # nosec: B108
@click.option("--table-ids", "-t", multiple=True)
@click.option(
    "--scopes",
    "-s",
    multiple=True,
    help="Scopes option has been disabled for now, only public data can be exported.",
)
@click.option("--size")
def export_geopackages_for(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
) -> None:
    """Export geopackages from postgres."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    with engine.begin() as connection:
        export_geopackages(
            connection, dataset_schema, output, table_ids=table_ids, scopes=[], size=size
        )


@export.command("csv")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.option("--output", "-o", default="/tmp")  # noqa: S108  # nosec: B108
@click.option("--table-ids", "-t", multiple=True)
@click.option(
    "--scopes",
    "-s",
    multiple=True,
    help="Scopes option has been disabled for now, only public data can be exported.",
)
@click.option("--size", type=int)
def export_csvs_for(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
) -> None:
    """Export csv files from postgres."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    with engine.begin() as connection:
        export_csvs(connection, dataset_schema, output, table_ids=table_ids, scopes=[], size=size)


@export.command("jsonlines")
@option_db_url
@option_schema_url
@argument_dataset_id
@click.option("--output", "-o", default="/tmp")  # noqa: S108  # nosec: B108
@click.option("--size")
@click.option("--table-ids", "-t", multiple=True)
@click.option(
    "--scopes",
    "-s",
    multiple=True,
    help="Scopes option has been disabled for now, only public data can be exported.",
)
@click.option("--size")
def export_jsonls_for(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    output: str,
    table_ids: list[str],
    scopes: list[str],
    size: int,
) -> None:
    """Export csv files from postgres."""
    engine = _get_engine(db_url)
    dataset_schema = _get_dataset_schema(dataset_id, schema_url)
    with engine.begin() as connection:
        export_jsonls(
            connection, dataset_schema, output, table_ids=table_ids, scopes=[], size=size
        )


@tocase.command("camel")
@click.argument("input_str")
def convert_to_camel_case(input_str: str) -> str:
    """Converts INPUT_STR to camel case."""
    click.echo(toCamelCase(input_str))


@tocase.command("snake")
@click.argument("input_str")
def convert_to_snake_case(input_str: str) -> str:
    """Converts INPUT_STR to snake case."""
    click.echo(to_snake_case(input_str))


@kafka.command()
@option_db_url
@option_schema_url
@argument_dataset_id
@click.option("--additional-schemas", "-a", multiple=True)
@click.option("--topics", "-t", multiple=True)
@click.option("--truncate-table", default=False, is_flag=True)
def consume(
    db_url: str,
    schema_url: str,
    dataset_id: str,
    additional_schemas: str,
    topics: Iterable[str],
    truncate_table: bool,
) -> None:
    """Consume kafka events."""
    # Late import, to prevent dependency on confluent-kafka for every cli user
    from schematools.events.consumer import consume_events

    engine = _get_engine(db_url)
    dataset_schemas = [_get_dataset_schema(dataset_id, schema_url)]
    for schema in additional_schemas:
        dataset_schemas.append(_get_dataset_schema(schema, schema_url))
    # Create connection, do not start a transaction.
    with engine.connect() as connection:
        consume_events(dataset_schemas, connection, topics, truncate=truncate_table)


if __name__ == "__main__":
    main()
