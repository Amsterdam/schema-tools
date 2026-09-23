from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import Column, MetaData, String, Table

from schematools import MAX_TABLE_NAME_LENGTH
from schematools.factories import (
    _build_m2m_indexes,
    _format_index_name,
    _numeric_datatype_scale,
    build_temporal_indexes,
    index_factory,
    tables_factory,
    views_factory,
)


def test_through_col_creation(engine, brk_schema, verblijfsobjecten_schema):
    """Prove that through tables are containing all fields from the schema definition.

    When a schema has a relation that contains not only the (composite) foreign key fields
    but also extra fields that are defined on the relation
    (e.g. beginGeldigheid and eindGeldigheid.), those fields should also end up
    in the resulting SQLAlchemy tables.
    """
    sa_tables = tables_factory(brk_schema)

    for test_table_name in [
        "stukdelen_isBronVoorAantekeningKadastraalObject",
        "aantekeningenrechten_heeftBetrokkenPersoon",
    ]:
        colum_names = {c.name for c in sa_tables[test_table_name].columns}
        assert {"begin_geldigheid", "eind_geldigheid"} < colum_names


def test_views_factory(engine, aardgasverbruik_schema):
    tables = tables_factory(aardgasverbruik_schema)
    views = views_factory(aardgasverbruik_schema, tables)
    for name in ["mraLiander", "mraStatistiekenPcranges", "aardgasverbruik"]:
        assert name in views


def test_views_factory_raises_on_mismatched_table_names(aardgasverbruik_schema):
    with pytest.raises(ValueError, match="mismatch"):
        views_factory(aardgasverbruik_schema, {"missing": object()})


def test_numeric_datatype_scale_detects_decimal_precision():
    result = _numeric_datatype_scale(0.01)

    assert result.precision == 12
    assert result.scale == 2
    assert _numeric_datatype_scale(2) is not None
    assert _numeric_datatype_scale(0.2).__name__ == "Numeric"


def test_build_temporal_indexes_returns_empty_for_missing_or_disabled_temporal(caplog):
    metadata = MetaData()
    table = Table("sample", metadata, Column("existing", String))
    disabled_temporal = SimpleNamespace(temporal=None)
    missing_field_temporal = SimpleNamespace(
        temporal=SimpleNamespace(
            temporal_fields=[SimpleNamespace(db_name="existing")],
        ),
        _temporal_range_field_ids=["missingField"],
    )

    assert build_temporal_indexes(table, disabled_temporal, "sample") == []

    indexes = build_temporal_indexes(table, missing_field_temporal, "sample")
    assert indexes == []
    assert "skipping temporal index creation" in caplog.text


def test_build_temporal_indexes_builds_combined_and_field_indexes():
    metadata = MetaData()
    table = Table(
        "sample",
        metadata,
        Column("start_date", String),
        Column("end_date", String),
    )
    dataset_table = SimpleNamespace(
        temporal=SimpleNamespace(
            temporal_fields=[
                SimpleNamespace(db_name="start_date"),
                SimpleNamespace(db_name="end_date"),
            ]
        ),
        _temporal_range_field_ids=["startDate", "endDate"],
    )

    indexes = build_temporal_indexes(table, dataset_table, "sample")

    assert len(indexes) == 3
    assert indexes[0].name == "sample_temporal_idx"
    assert {index.name for index in indexes[1:]} == {
        "sample_start_date_idx",
        "sample_end_date_idx",
    }


def test_build_m2m_indexes_skips_missing_tables_and_non_through_fields(caplog):
    metadata = MetaData()
    through_table = SimpleNamespace(
        db_name="through_table", through_fields=[SimpleNamespace(db_name="left_id")]
    )
    dataset_table = SimpleNamespace(
        fields=[
            SimpleNamespace(is_through_table=False),
            SimpleNamespace(is_through_table=True, through_table=through_table),
        ]
    )

    indexes = _build_m2m_indexes(metadata, dataset_table, "public")

    assert indexes == {}
    assert "skipping M2M index creation" in caplog.text


def test_build_m2m_indexes_creates_indexes_for_through_fields():
    metadata = MetaData(schema="public")
    Table("through_table", metadata, Column("left_id", String), Column("right_id", String))
    through_table = SimpleNamespace(
        db_name="through_table",
        through_fields=[
            SimpleNamespace(db_name="left_id"),
            SimpleNamespace(db_name="right_id"),
        ],
    )
    dataset_table = SimpleNamespace(
        fields=[SimpleNamespace(is_through_table=True, through_table=through_table)]
    )

    indexes = _build_m2m_indexes(metadata, dataset_table, "public")

    assert list(indexes) == ["through_table"]
    assert {index.name for index in indexes["through_table"]} == {
        "public.through_table_left_id_idx",
        "public.through_table_right_id_idx",
    }


def test_format_index_name_hashes_long_names():
    short_name = "short_idx"
    long_name = "x" * (MAX_TABLE_NAME_LENGTH + 1)

    assert _format_index_name(short_name) == short_name
    formatted = _format_index_name(long_name)
    assert formatted.endswith("_idx")
    assert len(formatted) < len(long_name)


def test_index_factory_uses_defaults_and_missing_table_fallback(monkeypatch, caplog):
    metadata = MetaData(schema="public")
    Table("sample", metadata, Column("id", String))
    dataset_table = SimpleNamespace(db_name="sample")

    monkeypatch.setattr(
        "schematools.factories._build_identifier_index",
        lambda table_object, dataset_table, db_table_name: (
            f"identifier:{table_object.name}:{db_table_name}"
        ),
    )
    monkeypatch.setattr("schematools.factories._build_fk_indexes", lambda *_args: ["fk"])
    monkeypatch.setattr("schematools.factories._build_geo_indexes", lambda *_args: ["geo"])
    monkeypatch.setattr(
        "schematools.factories.build_temporal_indexes", lambda *_args: ["temporal"]
    )
    monkeypatch.setattr(
        "schematools.factories._build_m2m_indexes", lambda *_args: {"through": ["m2m"]}
    )

    indexes = index_factory(dataset_table, metadata=metadata)

    assert indexes == {
        "sample": ["identifier:sample:sample", "fk", "geo", "temporal"],
        "through": ["m2m"],
    }

    missing = index_factory(dataset_table, metadata=MetaData())
    assert missing == {"through": ["m2m"]}
    assert "skipping index creation" in caplog.text
