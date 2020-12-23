from django.contrib.gis.db import models
from django_postgres_unlimited_varchar import UnlimitedCharField
from schematools.contrib.django.factories import model_factory, schema_models_factory
from schematools.contrib.django.models import (
    LooseRelationField,
    LooseRelationManyToManyField,
)


def test_model_factory_fields(afval_schema):
    """Prove that the fields from the schema will be generated"""
    table = afval_schema.tables[0]
    model_cls = model_factory(table, base_app_name="dso_api.dynamic_api")
    meta = model_cls._meta
    assert set(f.name for f in meta.get_fields()) == {
        "id",
        "cluster",
        "serienummer",
        "eigenaar_naam",
        "datum_creatie",
        "datum_leegmaken",
        "geometry",
    }
    assert meta.get_field("id").primary_key
    assert isinstance(meta.get_field("cluster_id"), models.ForeignKey)
    assert isinstance(meta.get_field("eigenaar_naam"), UnlimitedCharField)
    assert isinstance(meta.get_field("datum_creatie"), models.DateField)
    assert isinstance(meta.get_field("datum_leegmaken"), models.DateTimeField)
    geo_field = meta.get_field("geometry")
    assert geo_field.srid == 28992
    assert geo_field.db_index
    assert meta.app_label == afval_schema.id

    table_with_id_as_string = afval_schema.tables[1]
    model_cls = model_factory(
        table_with_id_as_string, base_app_name="dso_api.dynamic_api"
    )
    meta = model_cls._meta
    assert meta.get_field("id").primary_key
    assert isinstance(meta.get_field("id"), UnlimitedCharField)


def test_model_factory_relations(afval_schema):
    """Prove that relations between models can be resolved"""
    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            afval_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    cluster_fk = models["containers"]._meta.get_field("cluster")
    # Cannot compare using identity for dynamically generated classes
    assert (
        cluster_fk.related_model._table_schema.id == models["clusters"]._table_schema.id
    )


def test_model_factory_n_m_relations(meetbouten_schema):
    """Prove that n-m relations between models can be resolved"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            meetbouten_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    nm_ref = model_dict["metingen"]._meta.get_field("refereertaanreferentiepunten")
    assert isinstance(nm_ref, models.ManyToManyField)


def test_model_factory_sub_objects(parkeervakken_schema):
    """Prove that subobjects between models lead to extra child model"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            parkeervakken_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    assert "parkeervakken_regimes" in model_dict
    fields_dict = {f.name: f for f in model_dict["parkeervakken_regimes"]._meta.fields}
    assert "parent" in fields_dict
    assert isinstance(fields_dict["parent"], models.ForeignKey)


def test_model_factory_temporary_1_n_relation(ggwgebieden_schema):
    """Prove that extra relation fields are added to temporary relation"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            ggwgebieden_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    related_temporary_fields = {
        "ligtinstadsdeel_identificatie",
        "ligtinstadsdeel_volgnummer",
    }
    assert (
        set(f.name for f in model_dict["ggwgebieden"]._meta.fields)
        > related_temporary_fields
    )


def test_model_factory_temporary_n_m_relation(ggwgebieden_schema):
    """Prove that through table is created for n_m relation """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            ggwgebieden_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    # The through table is created
    through_table_name = "ggwgebieden_bestaatuitbuurten"
    assert through_table_name in model_dict

    # Through table has refs to both 'sides' and extra fields for the relation
    through_table_field_names = {
        "ggwgebieden",
        "bestaatuitbuurten",
        "identificatie",
        "volgnummer",
    }
    fields_dict = {f.name: f for f in model_dict[through_table_name]._meta.fields}

    assert set(fields_dict.keys()) > through_table_field_names
    for field_name in ("ggwgebieden", "bestaatuitbuurten"):
        assert isinstance(fields_dict[field_name], models.ForeignKey)


def test_model_factory_loose_relations(meldingen_schema):
    """Prove that a loose relation is created when column
    is part of relation definition (<dataset>:<table>:column)
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            meldingen_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    model_cls = model_dict["statistieken"]
    meta = model_cls._meta
    assert isinstance(meta.get_field("buurt"), LooseRelationField)


def test_model_factory_loose_relations_n_m_temporeel(woningbouwplannen_schema):
    """Prove that a loose relation is created when column
    is part of relation definition (<dataset>:<table>:column)
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            woningbouwplannen_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    model_cls = model_dict["woningbouwplan"]
    meta = model_cls._meta
    assert isinstance(meta.get_field("buurten"), LooseRelationManyToManyField)


def test_table_name_creation_n_m_relation(brk_schema):
    """Prove that through table name is looking at instance method db_name
    of the datasettableschema class to define it's name.
    Note: Adjust this test after db_name is getting value from Amsterdam schema
    specification.
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            brk_schema, base_app_name="dso_api.dynamic_api"
        )
    }
    # The through table is created
    # beware! the letter 't' is missing in the table name on purpose
    # currently the table name is maxed to 63 karakters minus 4 karakters
    # (because of the temp table which adde the postfix _new to the table name)
    through_table_name = "kadastraleobjecten_heeft_een_relatie_met_verblijfsobjec"
    assert through_table_name in model_dict
