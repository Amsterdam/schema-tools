import operator
from pathlib import Path
from typing import Any, Dict, List, cast

from click.testing import CliRunner
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import Session

from schematools.cli import create_all_objects


def test_create_all(here: Path, db_url: str, engine: Engine, dbsession: Session) -> None:
    # We're using the function scoped `dbsession` fixture for its side effect of automatically
    # dropping any tables we create. Beyond that we have no need for it.
    bbgaSchema = here / "files" / "bbga.json"
    runner = CliRunner()
    result = runner.invoke(
        create_all_objects,
        [f"--schema-url={bbgaSchema!s}", f"--db-url={db_url}"],
    )
    assert result.exit_code == 0

    inspector = Inspector.from_engine(engine)
    columns: List[Dict[str, Any]] = inspector.get_columns("bbga_kerncijfers")
    column_names = list(map(operator.itemgetter("name"), columns))

    # No "--relname-from-identifier" argument specified. Each relation specific column name
    # should have "_id! postfix.
    assert "indicator_definitie_id" in column_names


def test_create_all_relname_from_identifier(
    # We're using the function scoped `dbsession` fixture for its side effect of automatically
    # dropping any tables we create. Beyond that we have no need for it.
    here: Path,
    db_url: str,
    engine: Engine,
    dbsession: Session,
) -> None:
    bbgaSchema = here / "files" / "bbga.json"
    runner = CliRunner()
    result = runner.invoke(
        create_all_objects,
        [f"--schema-url={bbgaSchema!s}", f"--db-url={db_url}", "--relname-from-identifier"],
    )
    assert result.exit_code == 0

    inspector = Inspector.from_engine(engine)
    columns: List[Dict[str, Any]] = inspector.get_columns("bbga_kerncijfers")
    column_names = list(map(operator.itemgetter("name"), columns))

    # "--relname-from-identifier" argument specified, hence relation specific column name should
    # have postfix identical to value of `identifier` property in related column. In this case
    # that is "_variabele"
    assert "indicator_definitie_variabele" in column_names
