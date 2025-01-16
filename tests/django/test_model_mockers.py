from __future__ import annotations

import json

import pytest
from django.apps import apps
from django.contrib.gis.db.models import fields as geo_fields
from django.db.models import fields
from factory.base import FactoryMetaClass

from schematools.contrib.django.db import create_tables
from schematools.contrib.django.factories import (
    model_mocker_factory,
    schema_model_mockers_factory,
    schema_models_factory,
)
from schematools.contrib.django.models import Dataset
from schematools.types import DatasetTableSchema


@pytest.mark.django_db
def test_model_mocker_factory_registers_a_dynamic_model_in_the_app_config(afval_dataset) -> None:
    """Prove that the model_mocker_factory registers the model as Django app."""
    table_schema = afval_dataset.schema.tables[0]
    assert isinstance(table_schema, DatasetTableSchema)
    assert str(table_schema) == "<DatasetTableSchema: afvalwegingen.containers>"

    # We bootstrap the DynamicModel inside the model_mocker_factory by calling the model_factory,
    # which ends with app_config.register_model.
    # So we assert that the DynamicModel is indeed registered.
    model_mocker_factory(afval_dataset, table_schema, base_app_name="dso_api.dynamic_api")
    registered_models = [f"{m._meta.app_label}.{m._meta.model_name}" for m in apps.get_models()]
    assert "afvalwegingen.containers" in registered_models

    # The afval_dataset has two tables (containers, clusters),
    # so we check the second table (i.e. second model) too.
    table_schema_with_id_as_string = afval_dataset.schema.tables[1]
    assert isinstance(table_schema_with_id_as_string, DatasetTableSchema)
    assert str(table_schema_with_id_as_string) == "<DatasetTableSchema: afvalwegingen.clusters>"

    model_mocker_factory(
        afval_dataset, table_schema_with_id_as_string, base_app_name="dso_api.dynamic_api"
    )
    registered_models_updated = [
        f"{m._meta.app_label}.{m._meta.model_name}" for m in apps.get_models()
    ]
    assert "afvalwegingen.clusters" in registered_models_updated


@pytest.mark.skip(
    reason="""
The code covered by this test (in model_mocker_factory()) was passing a
mangled DatasetTableSchema to the model mocker factory, relying on
the assumption that snake_case and camelCase conversions are invertible in this
codebase in order to reconstruct the db_name in the mocker class.
These functions were not invertible and the code was breaking.

The invertibility has been fixed but we are still passing a mangled table-schema
to the mocker factory. Ideally we would like to find a solution were we pass the
table-schema as asserted in this test and extract the db_name in the mocker.
Therefore we keep this test around but skip it.
"""
)
@pytest.mark.django_db
def test_model_mocker_factory_sets_model_mocker_dataset_and_table_schema_through_dynamic_model(
    afval_dataset,
) -> None:
    """Prove that the model_mocker_factory creates a linked DynamicModelMocker.
    The mocker should be linked to a DynamicModel for a Dataset with a DatasetTableSchema
    """
    table_schema = afval_dataset.schema.tables[0]  # afvalwegingen.containers

    model_mocker_cls = model_mocker_factory(
        afval_dataset, table_schema, base_app_name="dso_api.dynamic_api"
    )
    # model_mocker_cls._meta is the DjangoModelFactory.Meta,
    # so get_model_class() gives the Django Model for which
    # the ModelFactory is implemented, which is the DynamicModel
    # for the given AmsSchema dataset.table
    AfvalwegingenContainersModel = model_mocker_cls._meta.get_model_class()
    assert (
        str(AfvalwegingenContainersModel)
        == "<class 'dso_api.dynamic_api.afvalwegingen.models.containers'>"
    )

    assert isinstance(AfvalwegingenContainersModel.get_dataset(), Dataset)
    assert AfvalwegingenContainersModel.get_dataset() == afval_dataset
    assert str(AfvalwegingenContainersModel.get_dataset()) == "afvalwegingen"

    #
    table_schema_dict = json.loads(table_schema.json())
    mocked_schema_dict = json.loads(AfvalwegingenContainersModel.table_schema().json())

    # For mocks, relations will be replaced by plain `_id` fields
    assert mocked_schema_dict["schema"]["properties"]["clusterId"] == {
        "description": "Cluster-ID",
        "faker": "nuller",
        "type": "string",
    }
    del mocked_schema_dict["schema"]["properties"]["clusterId"]
    del table_schema_dict["schema"]["properties"]["cluster"]
    table_schema_dict["schema"]["properties"]["id"]["type"] = "integer/autoincrement"

    # After removal of `cluster`/`cluster_id` the rest of the schema
    # should be identical
    assert mocked_schema_dict == table_schema_dict

    assert (
        str(AfvalwegingenContainersModel.table_schema())
        == "<DatasetTableSchema: afvalwegingen.containers>"
    )


@pytest.mark.django_db
def test_schema_model_mockers_factory(afval_dataset):
    """Prove that schema_model_mockers_factory works for an AmsSchema with 2 tables.

    Thus, 2 DynamicModel instances.
    """
    model_mockers = {
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    assert "containers" in model_mockers
    ContainersMocker = model_mockers["containers"]
    assert isinstance(ContainersMocker, FactoryMetaClass)
    assert str(ContainersMocker) == (
        "<containers_factory for <class 'dso_api.dynamic_api.afvalwegingen.models.containers'>>"
    )

    assert "clusters" in model_mockers
    ClustersMocker = model_mockers["clusters"]
    assert isinstance(ClustersMocker, FactoryMetaClass)
    assert str(ClustersMocker) == (
        "<clusters_factory for <class 'dso_api.dynamic_api.afvalwegingen.models.clusters'>>"
    )


@pytest.mark.django_db
def test_model_mocker_factory_fields(afval_dataset) -> None:
    """Prove that the model_mocker uses the fields from the database"""
    model_mockers = {
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }

    fields = {
        "id",
        "cluster",
        "serienummer",
        "eigenaar_naam",
        "datum_creatie",
        "datum_leegmaken",
        "geometry",
        "kortenaam",
    }

    ContainersMocker = model_mockers["containers"]
    assert {f.name for f in ContainersMocker._meta.get_model_class()._meta.get_fields()} == fields


@pytest.mark.django_db
def test_model_mocker_factory_records_count(afval_dataset) -> None:
    """Prove that the model_mocker generates the correct number of records."""
    model_mockers = {
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    ContainersMocker = model_mockers["containers"]
    Container = models["containers"]
    # Create the tables for the dataset, to be able te add records to it.
    create_tables(afval_dataset)
    ContainersMocker.create()
    assert Container.objects.count() == 1

    ContainersMocker.create_batch(2)
    assert Container.objects.count() == 3


@pytest.mark.django_db
def test_model_mocker_factory_field_types(afval_dataset) -> None:
    """Prove that the model_mocker generates the correct field types."""
    model_mockers = {
        cls._meta.get_model_class()._meta.model_name: cls
        for cls in schema_model_mockers_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    ContainersMocker = model_mockers["containers"]
    Container = models["containers"]
    # Create the tables for the dataset, to be able te add records to it.
    create_tables(afval_dataset)
    ContainersMocker.create()
    field_lookup = {f.name: f for f in Container.objects.first()._meta.get_fields()}

    for field_name, field_type in (
        ("datum_creatie", fields.DateField),
        ("datum_leegmaken", fields.DateTimeField),
        ("geometry", geo_fields.PointField),
    ):
        assert isinstance(field_lookup[field_name], field_type)
