from __future__ import annotations

import json
from collections import defaultdict

import pytest

from schematools.contrib.django.db import create_tables
from schematools.contrib.django.factories import schema_model_mockers_factory
from schematools.contrib.django.faker import get_field_factory
from schematools.contrib.django.faker.create import create_data_for
from schematools.contrib.django.faker.relate import relate_datasets
from schematools.naming import to_snake_case
from tests.django.utils import get_models


@pytest.mark.django_db
def test_mocking_creates_data(gebieden_schema, gebieden_dataset):
    size = 3
    create_tables(gebieden_dataset)
    create_data_for(gebieden_dataset, size=size)

    table_schemas = {
        to_snake_case(t.id): t for t in gebieden_schema.get_tables(include_through=True)
    }
    models = {}
    for cls in get_models(gebieden_dataset):
        models[cls._meta.model_name] = cls

    for model in models.values():
        assert model.objects.count() == size
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
    for cls in get_models(afvalwegingen_dataset):
        models[cls._meta.model_name] = cls

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
        for cls in get_models(dataset):
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
    relate_datasets(afvalwegingen_dataset, verblijfsobjecten_dataset, gebieden_dataset)

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

    models = {cls._meta.model_name: cls for cls in get_models(afvalwegingen_dataset)}
    mocked_fractie_codes = {o.afvalfractie for o in models["containers"].objects.all()}
    all_fractie_codes = {"Rest", "Glas", "Papier", "Plastic", "Textiel"}
    assert all_fractie_codes >= mocked_fractie_codes and mocked_fractie_codes


@pytest.mark.django_db
def test_mocking_add_temporal_fields_for_1n_relations(
    gebieden_dataset,
    gebieden_schema,
):
    """Prove that the separate temporal fields are updated correctly during the relate step."""
    models = {}
    create_tables(gebieden_dataset)
    for cls in schema_model_mockers_factory(gebieden_dataset, base_app_name="dso_api.dynamic_api"):
        cls.create_batch(5)
    for cls in get_models(gebieden_dataset):
        models[cls._meta.model_name] = cls

    relate_datasets(gebieden_dataset)

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

    models = {cls._meta.model_name: cls for cls in get_models(kadastraleobjecten_dataset)}

    relate_datasets(kadastraleobjecten_dataset)

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
        assert tmo.is_ontstaan_uit_kadastraalobject_id == ".".join(
            [
                tmo.is_ontstaan_uit_kadastraalobject_identificatie,
                str(tmo.is_ontstaan_uit_kadastraalobject_volgnummer),
            ]
        )
        assert tmo.kadastraleobjecten_id == ".".join(
            [
                tmo.kadastraleobjecten_identificatie,
                str(tmo.kadastraleobjecten_volgnummer),
            ]
        )


@pytest.mark.django_db
def test_mocking_with_shortname_on_relation(gebieden_dataset, gebieden_schema):
    """Prove that a relation with a shortname produces a correct field."""
    create_tables(gebieden_dataset)
    create_data_for(gebieden_dataset, size=10)

    models = {}
    for cls in get_models(gebieden_dataset):
        models[cls._meta.model_name] = cls

    fields = {f.name: f for f in models["bouwblokken"]._meta.get_fields()}
    # proves both the existence of the (long) fieldname + correct (short) db_column
    assert fields["ligt_in_buurt_met_te_lange_naam"].db_column == "lgt_in_brt_id"


@pytest.mark.django_db
def test_mocker_params_are_not_leaking(
    afvalwegingen_schema,
):
    """Prove that an enum definition on one field does not effect values on another field.

    Because we are not sure in what order the fields of a schema are processed,
    we have to conduct this test at a lower level.

    We take two fields of the afvalwegingen.container schema. The first field
    `afvalfractie` is a string and has an enum definition.
    The second field `containerlocatie id` is a string without an enum definition.

    We use the field_factory for the enum field first. Then we use the field factory
    for the second field. The resulting value should not be influenced by the
    enum definition of the first field.

    The second field uses the standard `pystr` provider that produces string
    with a length of 20 chars, so there should never be a change of overlap
    with the values in the enumeration.
    """
    table_schema = afvalwegingen_schema.get_table_by_id("containers")
    fractie_field_schema = table_schema.get_field_by_id("afvalfractie")
    locatie_field_schema = table_schema.get_field_by_id("containerlocatie id")
    fractie_field_factory = get_field_factory(fractie_field_schema)
    locatie_field_factory = get_field_factory(locatie_field_schema)
    fractie_provider_name = fractie_field_factory.provider
    locatie_provider_name = locatie_field_factory.provider
    fractie_faker = fractie_field_factory._get_faker()
    locatie_faker = locatie_field_factory._get_faker()
    fractie_provider = getattr(fractie_faker, fractie_provider_name)
    locatie_provider = getattr(locatie_faker, locatie_provider_name)
    elements = fractie_field_schema.json_data()["enum"]
    fractie_provider(elements=elements)
    locatie_value = locatie_provider()
    assert locatie_value not in elements
    assert len(locatie_value) not in {len(e) for e in elements}


@pytest.mark.django_db
def test_mocking_fills_nested_objects(
    kadastraleobjecten_dataset,
    kadastraleobjecten_schema,
):
    """Prove that mocking adds data for nested objects."""
    create_tables(kadastraleobjecten_dataset)
    mockers = schema_model_mockers_factory(
        kadastraleobjecten_dataset, base_app_name="dso_api.dynamic_api"
    )
    for mocker in mockers:
        mocker.create()

    models = {cls._meta.model_name: cls for cls in get_models(kadastraleobjecten_dataset)}

    kad_obj = models["kadastraleobjecten"].objects.first()
    # Random value in `soort_grootte` should be a valid json string.
    json.loads(kad_obj.soort_grootte)

    # Subfields should have been created, those could be null, so we cannot check if
    # the value is a string or has a certain length
    assert hasattr(kad_obj, "soort_cultuur_onbebouwd_code")
    assert hasattr(kad_obj, "soort_cultuur_onbebouwd_omschrijving")
