from __future__ import annotations

from typing import Any, Collection, Dict, List, Optional, Tuple, Type, TypeVar
from urllib.parse import urlparse

from django.apps import apps
from django.conf import settings
from django.contrib.gis.db import models
from django.contrib.gis.db import models as gis_models
from django.contrib.postgres.fields import ArrayField
from django.db.models import CheckConstraint, Q
from django.db.models.base import ModelBase

from schematools import SRID_3D
from schematools.contrib.django import app_config, signals
from schematools.contrib.django.fields import UnlimitedCharField
from schematools.naming import to_snake_case, toCamelCase
from schematools.types import DatasetFieldSchema, DatasetTableSchema

from .mockers import DynamicModelMocker
from .models import (
    CompositeForeignKeyField,
    Dataset,
    DynamicModel,
    LooseRelationField,
    LooseRelationManyToManyField,
)

TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]
M = TypeVar("M", bound=DynamicModel)
MODEL_CREATION_COUNTER = 1
MODEL_MOCKER_CREATION_COUNTER = 1

JSON_TYPE_TO_DJANGO = {
    "string": UnlimitedCharField,
    "integer": models.BigIntegerField,
    "integer/autoincrement": models.AutoField,
    "string/autoincrement": UnlimitedCharField,
    "datetime": models.DateTimeField,
    "number": models.FloatField,
    "boolean": models.BooleanField,
    "array": ArrayField,
    # Format variants of type string
    "date": models.DateField,
    "time": models.TimeField,
    "date-time": models.DateTimeField,
    "uri": models.URLField,
    "email": models.EmailField,
    "blob-azure": UnlimitedCharField,
    # "object" handled elsewhere, unless format is json
    # Format variant for type = object and format = json
    "json": models.JSONField,
    "/definitions/id": models.IntegerField,
    "/definitions/schema": UnlimitedCharField,
    "https://geojson.org/schema/Geometry.json": gis_models.GeometryField,
    "https://geojson.org/schema/Point.json": gis_models.PointField,
    "https://geojson.org/schema/MultiPoint.json": gis_models.MultiPointField,
    "https://geojson.org/schema/Polygon.json": gis_models.PolygonField,
    "https://geojson.org/schema/MultiPolygon.json": gis_models.MultiPolygonField,
    "https://geojson.org/schema/LineString.json": gis_models.LineStringField,
    "https://geojson.org/schema/MultiLineString.json": gis_models.MultiLineStringField,
    "https://geojson.org/schema/GeometryCollection.json": gis_models.GeometryCollectionField,
}


def remove_dynamic_models() -> None:
    """Erase model caches for dynamically generated models.
    This completely removes the models from the Django app registry.

    If your own code also holds references to models, these need to be removed separately.
    The :func:`is_dangling_model` function allows to check whether a model originated
    from a previous factory invocation.
    """
    virtual_apps = [
        name
        for name, config in apps.app_configs.items()
        if isinstance(config, app_config.VirtualAppConfig)
    ]
    for app_label in virtual_apps:
        del apps.all_models[app_label]
        del apps.app_configs[app_label]

    # See if there are dynamic models registered in other apps, erase them too.
    for app_label, app_models in apps.all_models.items():
        dynamic_models = [
            name for name, model in app_models.items() if issubclass(model, DynamicModel)
        ]
        for model_name in dynamic_models:
            del app_models[model_name]
            del apps.app_configs[app_label].models[model_name]

    # Allow other apps to clear their caches too
    signals.dynamic_models_removed.send(sender=None)

    # This also clears FK caches of other models that may have foreign keys:
    apps.clear_cache()

    # Allow code to detect whether a model is still alive despite having cleared all caches.
    global MODEL_CREATION_COUNTER
    MODEL_CREATION_COUNTER += 1


def is_dangling_model(model: type[DynamicModel]) -> bool:
    """Tell whether the model should have been removed, as everything reloaded."""
    return model.CREATION_COUNTER < MODEL_CREATION_COUNTER


def schema_models_factory(
    dataset: Dataset,
    tables: Collection[str] | None = None,
    base_app_name: str | None = None,
    base_model: type[M] = DynamicModel,
) -> list[type[M]]:
    """Generate Django models from the data of the schema."""
    return [
        model_factory(
            dataset=dataset,
            table_schema=table,
            base_app_name=base_app_name,
            base_model=base_model,
        )
        for table in dataset.schema.get_tables(include_nested=True, include_through=True)
        if tables is None or table.id in tables
    ]


def schema_model_mockers_factory(
    dataset: Dataset,
    tables: Optional[Collection[str]] = None,
    base_app_name: Optional[str] = None,
) -> List[Type[DynamicModelMocker]]:
    """Generate Django model mockers from the data of the schema."""
    return [
        model_mocker_factory(dataset=dataset, table_schema=table, base_app_name=base_app_name)
        for table in dataset.schema.get_tables(include_nested=True, include_through=True)
        if tables is None or table.id in tables
    ]


def _get_model_name(table_schema: DatasetTableSchema) -> str:
    """Returns model name for this table. Including version number, if needed."""
    # Using table_schema.python_name gives UpperCamelCased names, the old format is kept here.
    # This also keeps model names more readable and recognizable/linkable with db table names.
    model_name = to_snake_case(table_schema.id)
    if table_schema.dataset.version is not None and not table_schema.dataset.is_default_version:
        return f"{model_name}_{table_schema.dataset.version}"
    else:
        return model_name


def model_factory(
    dataset: Dataset,
    table_schema: DatasetTableSchema,
    base_app_name: str | None = None,
    base_model: type[M] = DynamicModel,
    meta_options: dict[str, Any] | None = None,
) -> type[M]:
    """Generate a Django model class from a JSON Schema definition."""
    dataset_schema = dataset.schema
    app_label = dataset_schema.id
    base_app_name = base_app_name or "dso_api.dynamic_api"
    module_name = f"{base_app_name}.{app_label}.models"
    is_temporal = table_schema.is_temporal
    display_field = table_schema.display_field

    # Generate fields
    fields = {}
    constraints = []

    for field in table_schema.get_fields(include_subfields=True):
        # skip schema field for now
        # skip nested tables and fields that are only added for temporality
        if (
            field.type.endswith("definitions/schema")
            or field.is_nested_table
            or field.is_temporal_range
        ):
            continue

        model_field = _model_field_factory(field)

        model_field.name = field.python_name  # Generate name, fix if needed.
        model_field.field_schema = field  # avoid extra lookups.
        fields[model_field.name] = model_field

        # Non-composite string identifiers may not contain forwardslashes, since this
        # breaks URL matching when they are used in URL paths.
        if field.is_primary and field.type == "string":
            # To make sure OneToOneField relations also support the '__contains' lookup,
            # that lookup type is registered with this field class in apps.py.
            constraints.append(
                CheckConstraint(
                    check=~Q(**{f"{model_field.name}__contains": "/"}),
                    name=f"{table_schema.db_name}_{field.db_name}_not_contains_slash",
                )
            )

    # Generate Meta part
    meta_cls = type(
        "Meta",
        (),
        {
            "managed": False,
            "db_table": table_schema.db_name,
            "app_label": app_label,
            "verbose_name": (table_schema.title or table_schema.id).capitalize(),
            "ordering": [idf.python_name for idf in table_schema.identifier_fields],
            "constraints": constraints,
            **(meta_options or {}),
        },
    )

    model_class = ModelBase(
        _get_model_name(table_schema),
        (base_model,),
        {
            **fields,
            "__doc__": table_schema.description or "",
            "_dataset": dataset,
            "_table_schema": table_schema,
            "_dataset_schema": dataset_schema,
            "_display_field": (display_field.python_name if display_field is not None else None),
            "_is_temporal": is_temporal,
            "CREATION_COUNTER": MODEL_CREATION_COUNTER,  # for debugging recreation
            "__module__": module_name,
            "Meta": meta_cls,
        },
    )

    app_config.register_model(app_label, model_class)
    return model_class


def _model_field_factory(field: DatasetFieldSchema) -> models.Field:
    """Construct the Django model field for a schema field."""
    if field.relation:
        return _fk_field_factory(field)
    elif field.nm_relation:
        return _nm_field_factory(field)
    else:
        return _basic_field_factory(field)


def _get_model_field_class(field: DatasetFieldSchema) -> type[models.Field]:
    type_ = field.type
    # reduce amsterdam schema refs to their fragment
    # only relevant for `/definitions/id` types atm.
    if type_.startswith(settings.SCHEMA_DEFS_URL):
        type_ = urlparse(type_).fragment

    try:
        return JSON_TYPE_TO_DJANGO[field.format or type_]
    except KeyError as e:
        raise RuntimeError(f"Field '{field.qualified_id}' has unsupported type: {type_}.") from e


def _get_basic_kwargs(field: DatasetFieldSchema) -> dict:
    """Common model field kwargs for all field types."""
    kwargs = {
        "primary_key": field.is_primary,
        "verbose_name": field.title,
        "help_text": field.description or "",  # also used by OpenAPI spec
        "db_column": field.db_name if field.db_name != field.python_name else None,
    }

    if not field.is_primary and field.nm_relation is None:
        # Primary can not be Null
        kwargs["null"] = not field.required

    return kwargs


def _basic_field_factory(field: DatasetFieldSchema) -> models.Field:
    """Construct a Django model field for a basic field type."""
    field_cls = _get_model_field_class(field)
    kwargs = _get_basic_kwargs(field)

    if issubclass(field_cls, ArrayField):
        # Array field
        item_type = field.get("entity", {}).get("type", "string")
        kwargs["base_field"] = JSON_TYPE_TO_DJANGO[item_type]()

    if issubclass(field_cls, gis_models.GeometryField):
        # Geometry field specials
        kwargs.update(
            {
                "srid": field.srid,
                "dim": (3 if field.srid in SRID_3D else 2),
                "geography": False,
                "db_index": True,
            }
        )

    return field_cls(**kwargs)


def _fk_field_factory(field: DatasetFieldSchema) -> models.ForeignKey:
    """Generate a Django ForeignKey field for a schema 1N relation field.

    This also takes composite-key relations into account,
    and "loose relations" where the field only references
    one part of the composite relation
    """
    to_field = _get_fk_to_field(field)
    kwargs = {
        "to": f"{field.related_table.dataset.id}.{_get_model_name(field.related_table)}",
        **_get_basic_kwargs(field),
        "on_delete": models.CASCADE if field.required else models.SET_NULL,
        "db_column": field.db_name,
        "db_constraint": False,  # don't enforce on database, not feasible for many datasets.
        "related_name": _get_fk_related_name(field),
    }

    if field.is_composite_key:
        # Make it easier to recognize the keys, e.g. in ``manage.py dump_models``.
        # For the most part, this is a tagging interface class.
        field_cls = CompositeForeignKeyField
        kwargs["to_fields"] = [field.python_name for field in field.related_fields]
    elif field.is_primary:
        # A primary key with a relation is typically a OneToOneField.
        # For a field named "id", using db_column="id" will work to retrieve the field there.
        # Internally, Django will still add an "id_id" field to the model to access the raw value.
        field_cls = models.OneToOneField
        kwargs["primary_key"] = True  # Note Django will use an INNER JOIN to retrieve the model.
        # Not using 'kwargs["parent_link"] = True', as model inheritance is not used here.
    elif field.is_loose_relation or to_field:
        # Points to the first part of a composite key (e.g. "identificatie").
        field_cls = LooseRelationField
        kwargs["to_field"] = to_field.python_name
    else:
        field_cls = models.ForeignKey

    return field_cls(**kwargs)


def _get_fk_related_name(field: DatasetFieldSchema) -> str:
    """Determine the name of the backwards relationship.

    If the linked table describes the other end of the relationship,
    this field will also be included in the model.
    """
    parent_table = field.table
    if parent_table.is_nested_table:
        # Won't ever show related name for internal tables
        return to_snake_case(parent_table["originalID"])
    elif parent_table.is_through_table:
        dataset_name = parent_table.dataset.db_name
        # This provides a reverse-link from each FK in the M2M table to the linked tables,
        # allowing to walk *inside* the M2M table instead of over it.
        # For debugging purposes these names are clarified depending on what direction
        # the key takes.
        m2m_field = parent_table.parent_table_field
        through_fields = parent_table["throughFields"]
        if field.name == through_fields[0]:
            # First model, can resemble the original field name,
            return f"{dataset_name}_rev_m2m_{to_snake_case(m2m_field.name)}"
        elif field.name == through_fields[1]:
            # Second model, can resemble the reverse name if it exists.
            m2m_reverse_name = m2m_field.reverse_relation
            if m2m_reverse_name is not None:
                # As the field exists on the second model,
                # let the reverse relation also reflect that.
                return f"{dataset_name}_rev_m2m_{to_snake_case(m2m_reverse_name.id)}"

        # By default, create something unique and recognizable.
        # The "m2m" would be snake_cased as m_2_m, so it's added afterwards.
        # The parent table ID already has the main field name included, but for tables
        # with a self-reference, the field is still added to guarantee uniqueness.
        return f"{dataset_name}_rev_m2m_" + to_snake_case(f"{parent_table.id}_via_{field.id}")
    elif (additional_relation := field.reverse_relation) is not None:
        # The relation is described by the other table, return it
        return additional_relation.id
    else:
        # Hide it as relation.
        # Note that for M2M relations, Django will replace this with "_tablename_fieldname_+",
        # as Django still uses the backwards relation internally.
        return "+"


def _get_fk_to_field(field: DatasetFieldSchema) -> DatasetFieldSchema | None:
    """Determine the "to_field" for the foreign key.

    This returns a value when the relation doesn't point to the targets's primary key,
    hence the "to_field" parameter is needed.

    The current implementation only works for the right-side of N-M relations at the moment,
    other relation types are still created as a loose relation field.
    """
    if (
        # HACK: This complicated logic is needed because self.field.is_loose_relation
        # has very mixed-up logic that handles things which the callers should have handled.
        # This makes it impossible to determine whether a through-table has loose relations.
        # Solving that turns out to be really complex and bring up more issues. However,
        # reading the NM-field does work, so at least one side of the relation can be fixed.
        (nm_field := field.table.parent_table_field) is not None
        and nm_field.is_loose_relation
        and field.id == field.table["throughFields"][1]
    ):
        # A FK in the through table might actually be a partial composite key.
        # When this is the case, make sure the to_field points to the right field.
        target_fields = nm_field.related_fields

        if target_fields[0].id != "id" and not target_fields[0].is_primary:
            return target_fields[0]
    elif field.is_loose_relation:
        # Loose relation points to the first field of a composite foreign key
        return field.related_table.identifier_fields[0]

    return None


def _nm_field_factory(field: DatasetFieldSchema) -> models.ManyToManyField:
    """Generate a Django ManyToManyField for the NM-relation.

    NOTE: this still generates a regular M2M relation for "loose m2m relations".
    """
    # TODO: this doesn't really take loose relations into account.
    # In practice, the schematools still has 2 regular foreignkey pairs,
    # # but also includes subfields for the composite key field.
    field_cls = LooseRelationManyToManyField if field.is_loose_relation else models.ManyToManyField
    through_table = field.through_table
    through_fields = [f.python_name for f in through_table.through_fields]

    if (additional_relation := field.reverse_relation) is not None:
        # The relation is described by the other table, return it
        related_name = additional_relation.id
    else:
        # Default: give it a name, but hide it as relation.
        # This becomes the models.ManyToManyRel field on the target model.
        related_name = f"rev_{field.table.python_name}_{field.python_name}+"

    kwargs = {
        "to": f"{field.related_table.dataset.id}.{_get_model_name(field.related_table)}",
        **_get_basic_kwargs(field),
        "related_name": related_name,
        "through": f"{through_table.dataset.id}.{_get_model_name(through_table)}",
        "through_fields": through_fields,
    }

    return field_cls(**kwargs)


def _simplify_table_schema_relations(table_schema: DatasetTableSchema):
    """Remove relation attributes from relation definitions.

    This prevents the creation of Django FK and M2M fields,
    because we don't want to use those during mocking.
    """
    new_table_data = table_schema.json_data()
    new_table_data["schema"]["properties"] = {}
    # For FK add postfix and simplify the field, for M2M skip the field.
    for field in table_schema.get_fields(include_subfields=True):
        field_definition = field.json_data()
        # By using the db_name of the field, we are using
        # the fact the an `_id` postfix is added for relation fields.
        db_name = field.db_name
        # Some autogenerated schemas already have an `autoincrement` type.
        if field.is_primary and not field.data["type"].endswith("autoincrement"):
            field_definition["type"] += "/autoincrement"
        if field.nm_relation is not None:
            continue
        if field.relation is not None:
            # in rare cases, a relation field is part
            # of the identifier, so we need to remove it.
            if field.id in table_schema.identifier:
                new_table_data["schema"]["identifier"].remove(field.id)
            field_definition.pop("relation", None)
            field_definition.pop("shortname", None)
            # convert object-type fields to strings
            if field.type == "object":
                field_definition["type"] = "string"
                del field_definition["properties"]

            # Add autoincrementing behaviour to the identifier fields
            # by adding a `faker` attribute
            field_definition["faker"] = "nuller"
        # We need to convert the db_name back to the proper camelCasing
        # that is being used in amsterdam schemas.
        new_table_data["schema"]["properties"][toCamelCase(db_name)] = field_definition
    return DatasetTableSchema(new_table_data, parent_schema=table_schema._parent_schema)


def model_mocker_factory(
    dataset: Dataset, table_schema: DatasetTableSchema, base_app_name: Optional[str] = None
) -> Type[DynamicModelMocker]:
    """Generate a Django model mocker class from a JSON Schema definition."""
    # delayed import so Faker/shapely etc are not loaded for every application,
    # but only when mocker functionality is used.
    from .faker import get_field_factory

    dataset_schema = dataset.schema
    app_label = f"{dataset_schema.id}"
    base_app_name = base_app_name or "dso_api.dynamic_api"

    # Bootstrap for the model_factory is implemented in the DynamicRouter, so on startup
    # of the DSO-API. We have no desire to bootstrap the DynamicModelMocker for DSO runtime,
    # just in the test suite and in the relevant management commands. As the DynamicModelMocker
    # wraps around the DynamicModel, we must initiate it here to feed to the DjangoModelFactory
    # Meta class:
    # https://factoryboy.readthedocs.io/en/stable/orms.html#the-djangomodelfactory-subclass.
    # This table_schema is stripped from relation info, we want to use simple
    # scalars for relations.
    stripped_table_schema = _simplify_table_schema_relations(table_schema)
    model_factory(dataset, stripped_table_schema, base_app_name=base_app_name)

    # Generate fields
    fields = {}
    for field in stripped_table_schema.get_fields(include_subfields=True):
        type_ = field.type
        # skip schema field for now
        if type_.endswith("definitions/schema"):
            continue
        # skip nested tables and fields that are only added for temporality
        if field.is_nested_table or field.is_temporal_range:
            continue

        # Generate name, fix if needed.
        field_name = field.python_name
        fields[field_name] = get_field_factory(field)

    # Generate Meta part
    meta_cls = type(
        "Meta",
        (),
        {
            "model": f"{app_label}.{_get_model_name(stripped_table_schema)}",
            "database": "default",
        },
    )

    # The `Params` class in an inner class on the mocker,
    # it is needed during lazy evaluation, to provide schema information
    # during the mocking process.
    params_cls = type("Params", (), {"table_schema": table_schema})

    model_mocker_class = type(
        f"{_get_model_name(stripped_table_schema)}_factory",
        (DynamicModelMocker,),
        {
            **fields,
            "Meta": meta_cls,
            "Params": params_cls,
            "__module__": "schematools.contrib.django.factories",
        },
    )

    return model_mocker_class
