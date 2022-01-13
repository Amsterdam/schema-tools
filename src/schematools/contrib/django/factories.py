from __future__ import annotations

from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type
from urllib.parse import urlparse

from django.apps import apps
from django.conf import settings
from django.contrib.gis.db import models
from django.db.models.base import ModelBase

from schematools.contrib.django import app_config, signals
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema
from schematools.utils import get_rel_table_identifier, to_snake_case

from .models import (
    FORMAT_MODELS_LOOKUP,
    JSON_TYPE_TO_DJANGO,
    CompositeForeignKeyField,
    Dataset,
    DynamicModel,
    LooseRelationField,
    LooseRelationManyToManyField,
    ObjectMarker,
)

TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]
MODEL_CREATION_COUNTER = 1


class RelationMaker:
    """Superclass to generate info for relation fields."""

    def __init__(
        self,
        dataset: DatasetSchema,
        table: DatasetTableSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ):
        self.dataset = dataset
        self.table = table
        self.field = field
        self._field_cls = field_cls
        self._args = args
        self._kwargs = kwargs
        self.relation = field.relation or field.nm_relation
        self.fk_relation = field.relation
        self.nm_relation = field.nm_relation

    @classmethod
    def fetch_maker(cls, field: DatasetFieldSchema):
        # determine type of relation (FKLoose, FK, M2M, LooseM2M)
        if field.relation:
            if field.is_loose_relation:
                return LooseFKRelationMaker
            else:
                return FKRelationMaker
        elif field.nm_relation:
            if field.is_loose_relation:
                return LooseM2MRelationMaker
            else:
                return M2MRelationMaker
        else:
            return None  # To signal this is not a relation

    def _make_related_classname(self, relation):
        related_dataset, related_table, *_ = relation.split(":")
        return f"{related_dataset}.{to_snake_case(related_table)}"

    def _make_through_classname(self, dataset_id, field_id):
        snakecased_fieldname = to_snake_case(field_id)
        through_table_id = get_rel_table_identifier(self.table.id, snakecased_fieldname)
        # Give Django app_label.model_name notation
        return f"{dataset_id}.{through_table_id}"

    @property
    def field_cls(self):
        return self._field_cls

    @property
    def field_args(self):
        return [self._make_related_classname(self.relation)]

    @property
    def field_kwargs(self):
        return self._kwargs

    @property
    def field_constructor_info(self):
        return self.field_cls, self.field_args, self.field_kwargs


class LooseFKRelationMaker(RelationMaker):
    @property
    def field_cls(self):
        return LooseRelationField

    @property
    def field_args(self):
        # NB overrides default behaviour in superclass
        return self._args

    @property
    def field_kwargs(self):
        target_table = self.field.related_table
        kwargs = {}
        kwargs["db_column"] = f"{to_snake_case(self.field.name)}_id"
        kwargs["relation"] = self.fk_relation
        kwargs["to_field"] = target_table.identifier[0]  # temporal identifier
        return {**super().field_kwargs, **kwargs}


class FKRelationMaker(RelationMaker):
    @property
    def field_cls(self):
        if self.field.is_composite_key:
            # Make it easier to recognize the keys, e.g. in ``manage.py dump_models``.
            return CompositeForeignKeyField
        else:
            return models.ForeignKey

    @property
    def field_args(self):
        return super().field_args + [models.CASCADE if self.field.required else models.SET_NULL]

    def _get_related_name(self):
        """Find the name of the backwards relationship.

        If the linked table describes the other end of the relationship,
        this field will also be included in the model.
        """
        if self.field._parent_table.has_parent_table:
            # Won't ever show related name for internal tables
            return to_snake_case(self.field._parent_table["originalID"])
        elif self.field._parent_table.is_through_table:
            # Need this for walking over the through table for the "_links" section.
            return to_snake_case(f"{self.field._parent_table.id}_through_{self.field.id}")
        elif (additional_relation := self.field.reverse_relation) is not None:
            # The relation is described by the other table, return it
            return additional_relation.id
        else:
            # Hide it as relation.
            return "+"

    @property
    def field_kwargs(self):
        # In schema foreign keys should be specified without _id,
        # but the db_column should be with _id
        kwargs = {
            **super().field_kwargs,
            "db_column": f"{to_snake_case(self.field.name)}_id",
            "db_constraint": False,
            "related_name": self._get_related_name(),
        }

        if self.field.is_composite_key:
            kwargs["to_fields"] = [to_snake_case(field.id) for field in self.field.subfields]

        return kwargs


class M2MRelationMaker(RelationMaker):
    @property
    def field_cls(self):
        return models.ManyToManyField

    @property
    def field_kwargs(self):
        snakecased_fieldname = to_snake_case(self.field.name)
        parent_table = to_snake_case(self.field._parent_table.name)

        if (additional_relation := self.field.reverse_relation) is not None:
            # The relation is described by the other table, return it
            related_name = additional_relation.id
        else:
            # Default: give it a name, but hide it as relation.
            related_name = f"{snakecased_fieldname}_{parent_table}+"

        return {
            **super().field_kwargs,
            "related_name": related_name,
            "through": self._make_through_classname(self.dataset.id, self.field.id),
            "through_fields": (parent_table, snakecased_fieldname),
        }


class LooseM2MRelationMaker(M2MRelationMaker):
    @property
    def field_cls(self):
        return LooseRelationManyToManyField


class FieldMaker:
    """Generate the field for a JSON-Schema property"""

    def __init__(
        self,
        field_cls: Type[models.Field],
        table_schema: DatasetTableSchema,
        value_getter: Callable[[DatasetSchema], Dict[str, Any]] = None,
        **kwargs,
    ):
        self.field_cls = field_cls
        self.table = table_schema
        self.value_getter = value_getter
        self.kwargs = kwargs
        self.modifiers = [getattr(self, an) for an in dir(self) if an.startswith("handle_")]

    def handle_basic(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        kwargs["primary_key"] = field.is_primary
        kwargs["help_text"] = field.description or ""  # also used by OpenAPI spec
        if not field.is_primary and field.nm_relation is None:
            # Primary can not be Null
            kwargs["null"] = not field.required
        if self.value_getter:
            kwargs = {**kwargs, **self.value_getter(dataset, field)}
        return field_cls, args, kwargs

    def handle_array(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        if field.data.get("type", "").lower() == "array" and not field.nm_relation:
            base_field, _ = JSON_TYPE_TO_DJANGO[field.data.get("entity", {}).get("type", "string")]
            kwargs["base_field"] = base_field()
        return field_cls, args, kwargs

    def handle_relation(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        try:
            relation_maker_cls = RelationMaker.fetch_maker(field)
        except ValueError as e:
            raise ValueError(
                f"Failed to construct field {dataset.id}.{field.table.id}.{field.name}: {e}"
            ) from e

        if relation_maker_cls is not None:
            relation_maker = relation_maker_cls(
                dataset, self.table, field, field_cls, *args, **kwargs
            )
            return relation_maker.field_constructor_info
        else:
            return field_cls, args, kwargs

    def handle_date(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        format_ = field.format
        if format_ is not None:
            field_cls = FORMAT_MODELS_LOOKUP[format_]
        return field_cls, args, kwargs

    def __call__(self, field: DatasetFieldSchema, dataset: DatasetSchema) -> TypeAndSignature:
        field_cls = self.field_cls
        kwargs = self.kwargs
        args = []

        for modifier in self.modifiers:
            field_cls, args, kwargs = modifier(dataset, field, field_cls, *args, **kwargs)

        return field_cls, args, kwargs


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


def is_dangling_model(model: Type[DynamicModel]) -> bool:
    """Tell whether the model should have been removed, as everything reloaded."""
    return model.CREATION_COUNTER < MODEL_CREATION_COUNTER


def schema_models_factory(
    dataset: Dataset,
    tables: Optional[Collection[str]] = None,
    base_app_name: Optional[str] = None,
) -> List[Type[DynamicModel]]:
    """Generate Django models from the data of the schema."""
    return [
        model_factory(dataset=dataset, table_schema=table, base_app_name=base_app_name)
        for table in dataset.schema.get_tables(include_nested=True, include_through=True)
        if tables is None or table.id in tables
    ]


def model_factory(
    dataset: Dataset, table_schema: DatasetTableSchema, base_app_name: Optional[str] = None
) -> Type[DynamicModel]:
    """Generate a Django model class from a JSON Schema definition."""
    dataset_schema = dataset.schema
    app_label = dataset_schema.id
    base_app_name = base_app_name or "dso_api.dynamic_api"
    module_name = f"{base_app_name}.{app_label}.models"
    display_field = (
        to_snake_case(table_schema.display_field) if table_schema.display_field else None
    )

    is_temporal = table_schema.is_temporal

    # Generate fields
    fields = {}
    for field in table_schema.fields:
        type_ = field.type
        # skip schema field for now
        if type_.endswith("definitions/schema"):
            continue
        # skip nested tables and fields that are only added for temporality
        if field.is_nested_table or field.is_temporal:
            continue
        # reduce amsterdam schema refs to their fragment
        if type_.startswith(settings.SCHEMA_DEFS_URL):
            type_ = urlparse(type_).fragment

        try:
            base_class, init_kwargs = JSON_TYPE_TO_DJANGO[type_]
        except KeyError as e:
            raise RuntimeError(
                f"Unable to parse {table_schema.id}: field '{field.name}'"
                f" has unsupported type: {type_}."
            ) from e

        if init_kwargs is None:
            init_kwargs = {}

        # Generate field object
        kls, args, kwargs = FieldMaker(base_class, table_schema, **init_kwargs)(
            field, dataset_schema
        )
        if kls is None or kls is ObjectMarker:
            # Some fields are not mapped into classes
            continue

        model_field = kls(*args, **kwargs)

        # Generate name, fix if needed.
        field_name = to_snake_case(field.name)
        model_field.name = field_name
        model_field.field_schema = field  # avoid extra lookups.
        fields[field_name] = model_field

    # Generate Meta part
    meta_cls = type(
        "Meta",
        (),
        {
            "managed": False,
            "db_table": table_schema.db_name(),
            "app_label": app_label,
            "verbose_name": table_schema.id.title(),
            "ordering": [to_snake_case(fn) for fn in table_schema.identifier],
        },
    )

    model_class = ModelBase(
        table_schema.model_name(),
        (DynamicModel,),
        {
            **fields,
            "__doc__": table_schema.description or "",
            "_dataset": dataset,
            "_dataset_schema": dataset_schema,
            "_table_schema": table_schema,
            "_display_field": display_field,
            "_is_temporal": is_temporal,
            "CREATION_COUNTER": MODEL_CREATION_COUNTER,  # for debugging recreation
            "__module__": module_name,
            "Meta": meta_cls,
        },
    )
    app_config.register_model(app_label, model_class)
    return model_class
