from datetime import datetime

from click.testing import CliRunner

from schematools.cli import import_schema
from schematools.models import Dataset, Table


def test_import_schema(engine, db_url, schemas_mock, dbsession):
    """Run ``schema import schema afvalwegingen``."""
    runner = CliRunner()
    result = runner.invoke(import_schema, [f"--db-url={db_url}", "afvalwegingen"])
    assert result.exit_code == 0, result.output

    # Check which data is stored in the database
    dataset: Dataset = dbsession.query(Dataset).get("afvalwegingen")
    assert dataset.date_created == datetime(2020, 1, 13)

    # Check that all tables are created.
    table_count: int = dbsession.query(Table).filter(Table.dataset_id == dataset.id).count()
    assert table_count == 4
