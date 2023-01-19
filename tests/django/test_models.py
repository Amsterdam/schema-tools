import pytest
from django.contrib.gis.db import models
from django.core.management import call_command
from django.db import IntegrityError, connection
from django.db.models.base import ModelBase
from django.db.models.fields import DateTimeField
from django_postgres_unlimited_varchar import UnlimitedCharField

from schematools.contrib.django.factories import model_factory, schema_models_factory
from schematools.contrib.django.models import (
    Dataset,
    LooseRelationField,
    LooseRelationManyToManyField,
)


@pytest.mark.django_db
def test_model_factory_fields(afval_dataset) -> None:
    """Prove that the fields from the schema will be generated"""
    table = afval_dataset.schema.tables[0]
    model_cls = model_factory(afval_dataset, table, base_app_name="dso_api.dynamic_api")
    meta = model_cls._meta
    assert meta.verbose_name == "Containers title"
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

    assert meta.get_field("eigenaar_naam").verbose_name == "Naam eigenaar"

    table_with_id_as_string = afval_dataset.schema.tables[1]
    model_cls = model_factory(
        dataset=afval_dataset,
        table_schema=table_with_id_as_string,
        base_app_name="dso_api.dynamic_api",
    )
    meta = model_cls._meta
    assert meta.get_field("id").primary_key
    assert isinstance(meta.get_field("id"), UnlimitedCharField)


@pytest.mark.django_db
def test_model_factory_table_name_no_versions(afval_dataset):
    """Prove that relations between models can be resolved"""
    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    Containers = models["containers"]
    assert Containers._meta.db_table == "afvalwegingen_containers"


@pytest.mark.django_db
def test_model_factory_table_name_default_version(afval_schema):
    """Prove that default dataset gets no version in table name"""
    afval_schema.data["default_version"] = "1.0.1"
    afval_schema.data["version"] = "1.0.1"
    afval_dataset = Dataset.create_for_schema(afval_schema)

    models = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(afval_dataset, base_app_name="dso_api.dynamic_api")
    }
    assert "containers" in models.keys()
    assert "containers_1_0_1" not in models.keys()
    Containers = models["containers"]
    assert Containers._meta.db_table == "afvalwegingen_containers"


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
def test_model_factory_n_m_relations(gebieden_dataset, meetbouten_dataset):
    """Prove that n-m relations between models can be resolved"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(meetbouten_dataset, base_app_name="dso_api.dynamic_api")
    }
    nm_ref = model_dict["metingen"]._meta.get_field("refereertaanreferentiepunten")
    assert isinstance(nm_ref, models.ManyToManyField)


@pytest.mark.django_db
def test_model_factory_pk_with_relation(here, aardgasverbruik_dataset):
    """Prove that primary keys with relations are supported."""
    call_command(
        "import_schemas", here / "files/datasets/aardgasverbruik.json", create_tables=True
    )
    MraLiander, PostcodeRangeModel = schema_models_factory(
        aardgasverbruik_dataset, base_app_name="dso_api.dynamic_api"
    )
    pk_field = PostcodeRangeModel._meta.pk
    assert isinstance(pk_field, models.OneToOneField)
    assert pk_field.db_column == "id"

    PostcodeRangeModel.objects.create(
        id_id="foobar",  # needs raw instance, must be mraLiander model otherwise.
        gemiddeld_verbruik=200,
    )
    # Prove that the actual database does use the "id" column:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM aardgasverbruik_mra_statistieken_pcranges")
        rows = dictfetchall(cursor)
    assert rows == [{"id": "foobar", "gemiddeld_verbruik": 200}]

    instance = PostcodeRangeModel.objects.get(id="foobar")  # new instance, no refresh_from_db()!
    assert instance.id_id == "foobar"  # read Django added "attname" attribute for raw data access
    with pytest.raises(MraLiander.DoesNotExist):
        assert instance.id  # Read OneToOneField descriptor that accesses the model value.


def dictfetchall(cursor):
    """Return all rows from a cursor as a dict"""
    # Django's connection.cursor() doesn't offer a way to pass RealDictCursor.
    # This code is straight from the Django docs:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


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
def test_model_factory_sub_objects_for_shortened_names(verblijfsobjecten_dataset, hr_dataset):
    """Prove that subobjects also work for shortened names in the schema"""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(hr_dataset, base_app_name="dso_api.dynamic_api")
    }

    # XXX Change: one field is now nested, the other is changed to a relation
    # Check a relation where the fieldname is intact and one where fieldname is shortened
    model = model_dict[
        "maatschappelijkeactiviteiten_heeft_sbi_activiteiten_voor_maatschappelijke_activiteit"
    ]
    fields_dict = {f.name: f for f in model._meta.fields}

    # Field is nested, should have a parent field
    assert isinstance(fields_dict["parent"], models.ForeignKey)

    model = model_dict["maatschappelijkeactiviteiten_heeft_sbi_activiteiten_voor_onderneming"]
    fields_dict = {f.name: f for f in model._meta.fields}

    # Field is a related, should have 2 FKs to both sides of the relation
    assert isinstance(fields_dict["maatschappelijkeactiviteiten"], models.ForeignKey)
    assert fields_dict["maatschappelijkeactiviteiten"].db_column == "activiteiten_id"

    heeft_sbi_voor_activiteit_voor_onder = fields_dict["heeft_sbi_activiteiten_voor_onderneming"]
    assert isinstance(heeft_sbi_voor_activiteit_voor_onder, models.ForeignKey)
    assert heeft_sbi_voor_activiteit_voor_onder.db_column == "sbi_voor_activiteit_id"


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
    model_fields = {f.name for f in model_dict["ggwgebieden"]._meta.fields}
    assert model_fields > related_temporary_fields


@pytest.mark.django_db
def test_model_factory_temporary_n_m_relation(ggwgebieden_dataset):
    """Prove that through table is created for n_m relation"""
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
        "bestaatuitbuurten_identificatie",
        "bestaatuitbuurten_volgnummer",
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
    and that the intermediate model contains the correct references to both
    associated tables.

    Loose m2m relations defined with an array of scalars or an array of
    single-property-objects generate the same output.
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
    intermediate_table = buurten_field.remote_field.through
    assert isinstance(buurten_field, LooseRelationManyToManyField)
    assert isinstance(intermediate_table, ModelBase)

    assert {x.name for x in intermediate_table._meta.get_fields()} == {
        "buurten",
        "id",
        "woningbouwplan",
    }

    buurten_as_scalar_field = meta.get_field("buurten_as_scalar")
    intermediate_table = buurten_as_scalar_field.remote_field.through
    assert isinstance(buurten_as_scalar_field, LooseRelationManyToManyField)
    assert isinstance(buurten_as_scalar_field.remote_field.through, ModelBase)

    assert {x.name for x in intermediate_table._meta.get_fields()} == {
        "buurten_as_scalar",
        "id",
        "woningbouwplan",
    }


@pytest.mark.django_db
def test_dataset_has_geometry_fields(afval_dataset, hr_dataset):
    """Prove that has_geometry_fields property is true if
    and only if a geometry field exists
    """
    assert afval_dataset.has_geometry_fields
    assert not hr_dataset.has_geometry_fields


@pytest.mark.django_db
def test_table_shortname(verblijfsobjecten_dataset, hr_dataset):
    """Prove that the shortnames definition for tables
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
        "hr_sbiactiviteiten",
        "hr_activiteiten_sbi_maatschappelijk",
        "hr_activiteiten_sbi_voor_activiteit",
        "hr_activiteiten_verblijfsobjecten",
        "hr_activiteiten_gevestigd_in",
        "hr_activiteiten_wordt_uitgeoefend_in_commerciele_vestiging",
    }

    assert db_table_names == {m._meta.db_table for m in model_dict.values()}


@pytest.mark.django_db
def test_column_shortnames_in_nm_throughtables(verblijfsobjecten_dataset, hr_dataset):
    """Prove that the shortnames definition for fields
    are showing up in the Django db_table definitions.
    We changed the table name to 'activiteiten'.
    And used a shortname for a nested and for a relation field.
    """

    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(hr_dataset, base_app_name="dso_api.dynamic_api")
    }

    db_colnames = {"activiteiten_id", "sbi_voor_activiteit_id"}
    model = model_dict["maatschappelijkeactiviteiten_heeft_sbi_activiteiten_voor_onderneming"]
    assert db_colnames == {f.db_column for f in model._meta.fields if f.db_column is not None}


@pytest.mark.django_db
def test_nested_objects_should_never_be_temporal(verblijfsobjecten_dataset):
    """Prove that a nested object is not marked as temporal."""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            verblijfsobjecten_dataset, base_app_name="dso_api.dynamic_api"
        )
    }

    assert not model_dict["verblijfsobjecten_gebruiksdoel"].is_temporal()


@pytest.mark.django_db
def test_temporal_subfields_are_skipped(verblijfsobjecten_dataset):
    """Prove that relation subfields are skipped when they are temporal.

    Verblijfsobjecten has as `beginGeldigheid` that is a `date-time`.
    The `ligtInBuurt` FK also has a `beginGeldigheid` but field that is a `date`.
    This `ligtInBuurt.beginGeldigheid` should not be used for model creation.
    If it does, is will "mask" the original `beginGeldigheid` field, this
    leads to a wrong Django field model type.
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            verblijfsobjecten_dataset, base_app_name="dso_api.dynamic_api"
        )
    }

    begin_geldigheid_field = model_dict["verblijfsobjecten"]._meta.get_field("begin_geldigheid")
    assert isinstance(begin_geldigheid_field, DateTimeField)


@pytest.mark.django_db
def test_non_composite_string_identifiers_use_slash_constraints(parkeervakken_dataset, here):
    call_command("import_schemas", here / "files/datasets/parkeervakken.json", create_tables=True)
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            parkeervakken_dataset, base_app_name="dso_api.dynamic_api"
        )
    }

    model = model_dict["parkeervakken"]

    with pytest.raises(
        IntegrityError,
        match=(
            r'^new row for relation "parkeervakken_parkeervakken" violates'
            r' check constraint "parkeervakken_parkeervakken_id_not_contains_slash".*'
        ),
    ):
        model.objects.create(id="forbidden/slash")


@pytest.mark.django_db
def test_model_factory_sub_object_is_flattened(kadastraleobjecten_dataset):
    """Prove that object type fields are 'flattened' in the model.

    The field `soortCultuurOnbebouwd` is an object field with subfields `code` and `omschrijving`.

    So, fields `soort_cultuur_onbebouwd_code`
        and `soort_cultuur_onbebouwd_omschrijving` should be generated.
    """
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            kadastraleobjecten_dataset, base_app_name="dso_api.dynamic_api"
        )
    }
    model_field_names = {f.name for f in model_dict["kadastraleobjecten"]._meta.fields}
    assert {
        "soort_cultuur_onbebouwd_code",
        "soort_cultuur_onbebouwd_omschrijving",
    } < model_field_names


@pytest.mark.django_db
def test_model_factory_sub_object_is_json(kadastraleobjecten_dataset):
    """Prove that object type fields are JSONField when `"format" = "json"`."""
    model_dict = {
        cls._meta.model_name: cls
        for cls in schema_models_factory(
            kadastraleobjecten_dataset, base_app_name="dso_api.dynamic_api"
        )
    }
    model_fields = {f.name: f for f in model_dict["kadastraleobjecten"]._meta.fields}
    assert isinstance(model_fields["soort_grootte"], models.JSONField)
