from collections import defaultdict

import pytest

from schematools.contrib.django.db import create_tables
from schematools.contrib.django.factories import (
    schema_model_mockers_factory,
    schema_models_factory,
)
from schematools.contrib.django.faker.create import create_data_for
from schematools.contrib.django.faker.relate import relate_datasets
from schematools.utils import to_snake_case


@pytest.mark.django_db
def test_mocking_creates_data(gebieden_schema, gebieden_dataset):
    create_tables(gebieden_dataset)
    create_data_for(gebieden_dataset, size=3)
    table_schemas = {
        to_snake_case(t.id): t for t in gebieden_schema.get_tables(include_through=True)
    }
    models = {}
    for cls in schema_models_factory(gebieden_dataset, base_app_name="dso_api.dynamic_api"):
        models[cls._meta.model_name] = cls

    for model in models.values():
        assert model.objects.count() == 3
        # For composite keys, a faker automatically kicks in to create the proper `id`
        if (table_schema := table_schemas[model._meta.model_name]).has_composite_key:
            for obj in model.objects.all():
                assert obj.id == ".".join(str(getattr(obj, fn)) for fn in table_schema.identifier)


@pytest.mark.django_db
def test_mocking_take_min_max_into_account(
    afvalwegingen_dataset, verblijfsobjecten_dataset, gebieden_dataset
):
    create_tables(gebieden_dataset)
    create_tables(verblijfsobjecten_dataset)
    create_tables(afvalwegingen_dataset)
    create_data_for(afvalwegingen_dataset, size=30)

    models = {}
    for cls in schema_models_factory(afvalwegingen_dataset, base_app_name="dso_api.dynamic_api"):
        models[cls._meta.model_name] = cls

    # The containertypes.volume field has a `maximum` of 20 and `minimum` of 5
    assert models["containertypes"].objects.filter(volume__gt=12, volume__lt=5).count() == 0


@pytest.mark.django_db
def test_mocking_add_ids_for_relations(
    afvalwegingen_schema,
    afvalwegingen_dataset,
    verblijfsobjecten_dataset,
    verblijfsobjecten_schema,
    gebieden_dataset,
    gebieden_schema,
):

    # NB. have to be in the correct order!
    datasets = (
        gebieden_dataset,
        verblijfsobjecten_dataset,
        afvalwegingen_dataset,
    )
    models = defaultdict(dict)
    for dataset in datasets:
        create_tables(dataset)
        for cls in schema_model_mockers_factory(dataset, base_app_name="dso_api.dynamic_api"):
            cls.create_batch(5)
        for cls in schema_models_factory(dataset, base_app_name="dso_api.dynamic_api"):
            models[dataset.name][cls._meta.model_name] = cls

    # The relations should be filled with None values
    for dataset_id, table_id, relation_ids in (
        ("afvalwegingen", "containers", ("cluster", "containertype")),
        ("afvalwegingen", "clusters", (to_snake_case("bagHoofdadresVerblijfsobject"),)),
        ("afvalwegingen", "wegingen", ("cluster",)),
    ):

        for relation_id in relation_ids:
            assert all(
                getattr(obj, relation_id) is None
                for obj in models[dataset_id][table_id].objects.all()
            )

    # Now add relation.
    relate_datasets(afvalwegingen_schema, verblijfsobjecten_schema, gebieden_schema)

    # Check if relations are added.
    for dataset_id, table_id, relation_ids in (
        ("afvalwegingen", "containers", ("cluster", "containertype")),
        ("afvalwegingen", "clusters", (to_snake_case("bagHoofdadresVerblijfsobject"),)),
        ("afvalwegingen", "wegingen", ("cluster",)),
    ):

        for relation_id in relation_ids:
            assert all(
                getattr(obj, relation_id) is not None
                for obj in models[dataset_id][table_id].objects.all()
            )


@pytest.mark.django_db
def test_mocking_uses_enum(
    afvalwegingen_schema,
    afvalwegingen_dataset,
    verblijfsobjecten_dataset,
    gebieden_dataset,
    verblijfsobjecten_schema,
):
    """Prove that mocking only uses values from an enum in a field definition."""
    create_tables(gebieden_dataset)
    create_tables(verblijfsobjecten_dataset)
    create_tables(afvalwegingen_dataset)
    mockers = schema_model_mockers_factory(
        afvalwegingen_dataset, base_app_name="dso_api.dynamic_api"
    )
    for mocker in mockers:
        mocker.create_batch(5)

    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            afvalwegingen_dataset, base_app_name="dso_api.dynamic_api"
        )
    }
    mocked_fractie_codes = {o.afvalfractie for o in models["containers"].objects.all()}
    all_fractie_codes = {"Rest", "Glas", "Papier", "Plastic", "Textiel"}
    assert all_fractie_codes >= mocked_fractie_codes and mocked_fractie_codes


@pytest.mark.django_db
def test_mocking_add_temporal_fields_for_1n_relations(
    gebieden_dataset,
    gebieden_schema,
):
    """Prove that the separete temporal fields are updated correctly during the relate step."""
    models = {}
    create_tables(gebieden_dataset)
    for cls in schema_model_mockers_factory(gebieden_dataset, base_app_name="dso_api.dynamic_api"):
        cls.create_batch(5)
    for cls in schema_models_factory(gebieden_dataset, base_app_name="dso_api.dynamic_api"):
        models[cls._meta.model_name] = cls

    relate_datasets(gebieden_schema)

    for bb in models["bouwblokken"].objects.all():
        bb.ligt_in_buurt_id = ".".join(
            [
                bb.ligt_in_buurt_identificatie,
                str(bb.ligt_in_buurt_volgnummer),
            ]
        )


@pytest.mark.django_db
def test_mocking_adds_nm_relations(
    kadastraleobjecten_dataset,
    kadastraleobjecten_schema,
):
    """Prove that mocking adds records to the through_model for an nm-relation."""
    create_tables(kadastraleobjecten_dataset)
    mockers = schema_model_mockers_factory(
        kadastraleobjecten_dataset, base_app_name="dso_api.dynamic_api"
    )
    for mocker in mockers:
        mocker.create_batch(20)

    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            kadastraleobjecten_dataset, base_app_name="dso_api.dynamic_api"
        )
    }

    relate_datasets(kadastraleobjecten_schema)

    source_model = models["kadastraleobjecten"]
    through_model = models["kadastraleobjecten_is_ontstaan_uit_kadastraalobject"]
    through_model_objects = through_model.objects.all()
    source_ids = {o.id for o in source_model.objects.all()}
    through_source_ids = {o.kadastraleobjecten_id for o in through_model_objects}
    through_target_ids = {o.is_ontstaan_uit_kadastraalobject_id for o in through_model_objects}

    # Records in the through table should point to the ids in the source model.
    assert through_target_ids <= source_ids
    assert through_source_ids <= source_ids

    # Also prove that the separately stored temporal fields are correct
    for tmo in through_model_objects:
        tmo.is_ontstaan_uit_kadastraalobject_id = ".".join(
            [
                tmo.is_ontstaan_uit_kadastraalobject_identificatie,
                str(tmo.is_ontstaan_uit_kadastraalobject_volgnummer),
            ]
        )
        tmo.kadastraleobjecten_id = ".".join(
            [
                tmo.kadastraleobjecten_identificatie,
                str(tmo.kadastraleobjecten_volgnummer),
            ]
        )
