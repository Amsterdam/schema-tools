import pytest
from django.apps import apps
from factory.base import FactoryMetaClass

from schematools.contrib.django.factories import model_mocker_factory, schema_model_mockers_factory
from schematools.contrib.django.mockers import DynamicModelMocker
from schematools.types import DatasetTableSchema


@pytest.mark.django_db
def test_model_mocker_factory_registers_a_dynamic_model_in_the_app_config(afval_dataset) -> None:
    """Prove that the model_mocker_factory registers the model as Django app"""
    table = afval_dataset.schema.tables[0]
    assert isinstance(table, DatasetTableSchema)
    assert str(table) == "<DatasetTableSchema: afvalwegingen.containers>"

    # We bootstrap the DynamicModel inside the model_mocker_factory by calling the model_factory,
    # which ends with app_config.register_model. So we assert that the DynamicModel is indeed registered.
    model_mocker_cls = model_mocker_factory(
        afval_dataset, table, base_app_name="dso_api.dynamic_api"
    )
    registered_models = [f"{m._meta.app_label}.{m._meta.model_name}" for m in apps.get_models()]
    assert "afvalwegingen.containers" in registered_models

    # The afval_dataset has two tables (containers, clusters), so we check the second table (i.e. second model) too.
    table_with_id_as_string = afval_dataset.schema.tables[1]
    assert isinstance(table_with_id_as_string, DatasetTableSchema)
    assert str(table_with_id_as_string) == "<DatasetTableSchema: afvalwegingen.clusters>"

    model_mocker_cls_2 = model_mocker_factory(
        afval_dataset, table_with_id_as_string, base_app_name="dso_api.dynamic_api"
    )
    registered_models_updated = [
        f"{m._meta.app_label}.{m._meta.model_name}" for m in apps.get_models()
    ]
    assert "afvalwegingen.clusters" in registered_models_updated


@pytest.mark.django_db
def test_schema_model_mockers_factory(afval_dataset):
    """Prove that schema_model_mockers_factory works"""
    model_mockers = {
        # cls._meta is the DjangoModelFactory.Meta, so get_model_class() gives the Django Model for which
        # the ModelFactory is implemented, which is the DynamicModel for the given AmsSchema dataset.table
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    assert "containers" in model_mockers.keys()
    ContainersMocker = model_mockers["containers"]
    assert type(ContainersMocker) == FactoryMetaClass
    assert str(ContainersMocker) == (
        "<dso_api.dynamic_api.afvalwegingen.mockers.containers"
        + " for <class 'dso_api.dynamic_api.afvalwegingen.models.containers'>>"
    )
    assert ContainersMocker.CREATION_COUNTER == 1

    assert "clusters" in model_mockers.keys()
    ClustersMocker = model_mockers["clusters"]
    assert type(ClustersMocker) == FactoryMetaClass
    assert str(ClustersMocker) == (
        "<dso_api.dynamic_api.afvalwegingen.mockers.clusters"
        + " for <class 'dso_api.dynamic_api.afvalwegingen.models.clusters'>>"
    )
    assert ClustersMocker.CREATION_COUNTER == 1


@pytest.mark.django_db
def test_model_mocker_factory_fields(afval_dataset) -> None:
    """Prove that the model_mocker uses the fields from the schema"""
    model_mockers = {
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    # assert {f.name for f in meta.get_fields()} == {
    #     "id",
    #     "cluster",
    #     "serienummer",
    #     "eigenaar_naam",
    #     "datum_creatie",
    #     "datum_leegmaken",
    #     "geometry",
    # }
