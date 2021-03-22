import pytest
from django.contrib.gis.db import models
from django.db.models.base import ModelBase
from django_postgres_unlimited_varchar import UnlimitedCharField

from schematools.contrib.django.factories import model_factory, schema_models_factory
from schematools.contrib.django.models import LooseRelationField, LooseRelationManyToManyField


@pytest.mark.django_db
def test_model_factory_fields(afval_dataset):
    """Prove that the fields from the schema will be generated"""
    table = afval_dataset.schema.tables[0]
    model_cls = model_factory(afval_dataset, table, base_app_name="dso_api.dynamic_api")
    meta = model_cls._meta
    assert {f.name for f in meta.get_fields()} == {
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
    assert meta.app_label == afval_dataset.schema.id

    table_with_id_as_string = afval_dataset.schema.tables[1]
    model_cls = model_factory(
        dataset=afval_dataset, table=table_with_id_as_string, base_app_name="dso_api.dynamic_api"
    )
    meta = model_cls._meta
    assert meta.get_field("id").primary_key
    assert isinstance(meta.get_field("id"), UnlimitedCharField)


@pytest.mark.django_db
def test_model_factory_relations(afval_dataset):
    """Prove that relations between models can be resolved"""
    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    cluster_fk = models["containers"]._meta.get_field("cluster")
    # Cannot compare using identity for dynamically generated classes
    assert cluster_fk.related_model._table_schema.id == models["clusters"]._table_schema.id


@pytest.mark.django_db
def test_model_factory_n_m_relations(meetbouten_dataset, gebieden_dataset):
    """Prove that n-m relations between models can be resolved"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(meetbouten_dataset, base_app_name="dso_api.dynamic_api")
    }
    nm_ref = model_dict["metingen"]._meta.get_field("refereertaanreferentiepunten")
    assert isinstance(nm_ref, models.ManyToManyField)


@pytest.mark.django_db
def test_model_factory_sub_objects(parkeervakken_dataset):
    """Prove that subobjects between models lead to extra child model"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            parkeervakken_dataset, base_app_name="dso_api.dynamic_api"
        )
    }
    assert "parkeervakken_regimes" in model_dict

    fields_dict = {f.name: f for f in model_dict["parkeervakken_regimes"]._meta.fields}
    assert "parent" in fields_dict
    assert isinstance(fields_dict["parent"], models.ForeignKey)


@pytest.mark.django_db
def test_model_factory_sub_objects_for_shortened_names(hr_dataset, verblijfsobjecten_dataset):
    """Prove that subobjects also work for shortened names in the schema"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(hr_dataset, base_app_name="dso_api.dynamic_api")
    }

    # Check a relation where the fieldname is intact and one where fieldname is shortened
    for fieldname in (
        "maatschappelijkeactiviteiten_heeft_sbi_activiteiten_voor_maatschappelijke_activiteit",
        "maatschappelijkeactiviteiten_heeft_sbi_activiteiten_voor_onderneming",
    ):
        assert fieldname in model_dict
        fields_dict = {f.name: f for f in model_dict[fieldname]._meta.fields}
        assert "parent" in fields_dict
        assert isinstance(fields_dict["parent"], models.ForeignKey)


@pytest.mark.django_db
def test_model_factory_temporary_1_n_relation(ggwgebieden_dataset):
    """Prove that extra relation fields are added to temporary relation"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(ggwgebieden_dataset, base_app_name="dso_api.dynamic_api")
    }
    related_temporary_fields = {
        "ligtinstadsdeel_identificatie",
        "ligtinstadsdeel_volgnummer",
    }
    assert {f.name for f in model_dict["ggwgebieden"]._meta.fields} > related_temporary_fields


@pytest.mark.django_db
def test_model_factory_temporary_n_m_relation(ggwgebieden_dataset):
    """Prove that through table is created for n_m relation """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(ggwgebieden_dataset, base_app_name="dso_api.dynamic_api")
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


@pytest.mark.django_db
def test_model_factory_loose_relations(meldingen_dataset, gebieden_dataset):
    """Prove that a loose relation is created when column
    is part of relation definition (<dataset>:<table>:column)
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(meldingen_dataset, base_app_name="dso_api.dynamic_api")
    }
    model_cls = model_dict["statistieken"]
    meta = model_cls._meta
    assert isinstance(meta.get_field("buurt"), LooseRelationField)


@pytest.mark.django_db
def test_model_factory_loose_relations_n_m_temporeel(woningbouwplannen_dataset, gebieden_dataset):
    """Prove that a loose relation is created when column
    is part of relation definition (<dataset>:<table>:column)
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            woningbouwplannen_dataset, base_app_name="dso_api.dynamic_api"
        )
    }
    model_cls = model_dict["woningbouwplan"]
    meta = model_cls._meta
    buurten_field = meta.get_field("buurten")
    assert isinstance(buurten_field, LooseRelationManyToManyField)
    assert isinstance(buurten_field.remote_field.through, ModelBase)
    buurten_as_scalar_field = meta.get_field("buurten_as_scalar")
    assert isinstance(buurten_as_scalar_field, LooseRelationManyToManyField)
    assert isinstance(buurten_as_scalar_field.remote_field.through, ModelBase)


@pytest.mark.django_db
def test_table_shortname(hr_dataset, verblijfsobjecten_dataset):
    """Prove that the shortnames definition for tables and fields
    are showing up in the Django db_table definitions.
    We changed the table name to 'activiteiten'.
    And used a shortname for a nested and for a relation field.
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(hr_dataset, base_app_name="dso_api.dynamic_api")
    }
    db_table_names = {
        "hr_activiteiten",
        "hr_activiteiten_sbi_maatschappelijk",
        "hr_activiteiten_heeft_sbi_activiteiten_voor_onderneming",
        "hr_activiteiten_verblijfsobjecten",
        "hr_activiteiten_wordt_uitgeoefend_in_commerciele_vestiging",
    }

    assert db_table_names == set(m._meta.db_table for m in model_dict.values())
