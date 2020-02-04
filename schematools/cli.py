import os
from sqlalchemy import create_engine
import click

from .db import fetch_table_names
from .create_schema import fetch_schema_for


@click.group()
def schema():
    pass


@schema.group()
def create():
    pass


@schema.group()
def fetch():
    pass


@fetch.command()
@click.option("--db-url", help="DSN of database")
def tablenames(db_url):
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    print("\n".join(fetch_table_names(engine)))


@create.command("schema")
@click.option("--prefix", "-p", help="Tables have prefix that needs to be stripped")
@click.option("--db-url", help="DSN of database")
@click.argument("dataset_id")
@click.argument("tables", nargs=-1)
def _schema(prefix, db_url, dataset_id, tables):
    engine = create_engine(db_url or os.getenv("DATABASE_URL"))
    print(fetch_schema_for(engine, dataset_id, tables, prefix))
