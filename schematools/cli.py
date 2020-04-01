import os
from sqlalchemy import create_engine, MetaData
import click
import ndjson
from shapely.geometry import shape
import jsonschema as js
import requests

from amsterdam_schema.utils import schema_def_from_url

from .db import (
    fetch_table_names,
    create_rows,
    create_meta_tables,
    create_meta_table_data,
)
from .create_schema import fetch_schema_for, fetch_schema_from_relational_schema


SCHEMA_URL = os.getenv("SCHEMA_URL")
metadata = MetaData()


def fetch_rows(fh, srid):
    data = ndjson.load(fh)
    for row in data:
        row["geometry"] = f"SRID={srid};{shape(row['geometry']).wkt}"
        yield row


@click.group()
def schema():
    pass


@schema.group()
def generate():
    pass


@schema.group()
def fetch():
    pass


@schema.command()
@click.argument("meta_schema_url")
@click.argument("schema_url")
def validate(meta_schema_url, schema_url):
    response = requests.get(meta_schema_url)
    schema = response.json()
    response = requests.get(schema_url)
    instance = response.json()
    js.validate(instance=instance, schema=schema)


@fetch.command()
@click.option("--db-url", help="DSN of database")
def tablenames(db_url):
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    print("\n".join(fetch_table_names(engine)))


@schema.command()
@click.option("--prefix", "-p", help="Tables have prefix that needs to be stripped")
@click.option("--db-url", help="DSN of database")
@click.argument("dataset_id")
@click.argument("tables", nargs=-1)
def introspect(prefix, db_url, dataset_id, tables):
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    print(fetch_schema_for(engine, dataset_id, tables, prefix))


@fetch.command()
@click.option("--db-url", help="DSN of database")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("ndjson_path")
def records(db_url, schema_name, table_name, ndjson_path):
    # Add batching for rows.
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    dataset_schema = schema_def_from_url(SCHEMA_URL, schema_name)
    srid = dataset_schema["crs"].split(":")[-1]
    with open(ndjson_path) as fh:
        data = list(fetch_rows(fh, srid))
    create_rows(engine, metadata, dataset_schema, table_name, data)


@generate.command()
@click.option("--db-url", help="DSN of database")
@click.argument("schema_name")
def arschema(db_url, schema_name):
    # Add drop or not flag
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    try:
        dataset_schema = schema_def_from_url(SCHEMA_URL, schema_name)
    except KeyError:
        click.echo(f"Schema {schema_name} not found.")

    create_meta_tables(engine)
    create_meta_table_data(engine, dataset_schema)


@fetch.command("schema")
@click.option("--db-url", help="DSN of database")
@click.argument("dataset_id")
def _schema(db_url, dataset_id):
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    json_schema = fetch_schema_from_relational_schema(engine, dataset_id)
    click.echo(json_schema)
