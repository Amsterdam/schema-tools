from __future__ import annotations

from schematools.factories import tables_factory, views_factory


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
