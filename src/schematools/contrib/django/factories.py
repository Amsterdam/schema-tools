from __future__ import annotations

from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type
from urllib.parse import urlparse

from django.apps import apps
from django.conf import settings
from django.contrib.gis.db import models
from django.db.models.base import ModelBase
from more_itertools import first

from schematools.contrib.django import app_config, signals
from schematools.types import DatasetFieldSchema, DatasetSchema, DatasetTableSchema
from schematools.utils import get_rel_table_identifier, to_snake_case

from .models import (
    FORMAT_MODELS_LOOKUP,
    JSON_TYPE_TO_DJANGO,
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
    def fetch_relation_parts(cls, relation):
        dataset_id, table_id, *_ = relation.split(":")
        return dataset_id, table_id

    @classmethod
    def is_loose_relation(cls, relation, dataset, field):
        """Determine if relation is loose or not."""

        related_dataset_id, related_table_id = cls.fetch_relation_parts(relation)
        related_dataset = dataset.get_dataset_schema(related_dataset_id)
        related_table = related_dataset.get_table_by_id(related_table_id)

        # Short-circuit for non-temporal or on-the-fly (through or nested) schemas
        if (
            not related_table.is_temporal
            or field._parent_table.is_through_table
            or field._parent_table.is_nested_table
        ):
            return False

        # So, target-side of relation is temporal
        # Determine fieldnames used for temporal
        sequence_identifier = related_table.temporal.identifier
        # Table identifier is mandatory and always contains at least one field
        identifier = first(related_table.identifier)

        # If temporal, this implicates that the type is not a scalar
        # but needs to be more complex (object) or array_of_objects
        if field.type in ["string", "integer"] or field.is_array_of_scalars:
            return True

        sequence_field = related_table.get_field_by_id(sequence_identifier)
        identifier_field = related_table.get_field_by_id(identifier)

        if field.is_array_of_objects:
            properties = field.field_items["properties"]
        elif field.is_object:
            properties = field["properties"]
        else:
            raise ValueError("Relations should have string/array/object type")

        source_type_set = {
            (prop_name, prop_val["type"]) for prop_name, prop_val in properties.items()
        }
        destination_type_set = {
            (sequence_field.name, sequence_field.type),
            (identifier_field.name, identifier_field.type),
        }

        return source_type_set != destination_type_set

    @classmethod
    def fetch_maker(cls, dataset, field):
        # determine type of relation (FKLoose, FK, M2M, LooseM2M)
        if field.relation:
            if cls.is_loose_relation(field.relation, dataset, field):
                return LooseFKRelationMaker
            else:
                return FKRelationMaker
        elif field.nm_relation:
            if cls.is_loose_relation(field.nm_relation, dataset, field):
                return LooseM2MRelationMaker
            else:
                return M2MRelationMaker
        else:
            return None  # To signal this is not a relation

    def _make_related_classname(self, relation):
        related_dataset, related_table = [
            to_snake_case(part) for part in self.fetch_relation_parts(relation)
        ]
        return f"{related_dataset}.{related_table}"

    def _make_through_classname(self, dataset_id, field_id):
        snakecased_fieldname = to_snake_case(field_id)
        through_table_id = get_rel_table_identifier(
            len(dataset_id) + 1, self.table.id, snakecased_fieldname
        )
        # dso-api expects the dataset_id separated from the table_id by a point
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
        kwargs = {}
        kwargs["db_column"] = f"{to_snake_case(self.field.name)}_id"
        kwargs["relation"] = self.fk_relation
        return {**super().field_kwargs, **kwargs}


class FKRelationMaker(RelationMaker):
    @property
    def field_cls(self):
        return models.ForeignKey

    @property
    def field_args(self):
        return super().field_args + [models.CASCADE if self.field.required else models.SET_NULL]

    def _fetch_related_name_for_backward_relations(self):
        related_name = None
        try:
            _, related_table_id = self.fetch_relation_parts(self.relation)
            table = self.dataset.get_table_by_id(related_table_id)
            for name, relation in table.relations.items():
                if (
                    relation["table"] == self.field.table.id
                    and relation["field"] == self.field.name
                ):
                    related_name = name
                    break
        except ValueError:
            pass

        return related_name or "+"

    @property
    def field_kwargs(self):
        kwargs = {}
        # In schema foreign keys should be specified without _id,
        # but the db_column should be with _id
        kwargs["db_column"] = f"{to_snake_case(self.field.name)}_id"
        kwargs["db_constraint"] = False  # relation is not mandatory
        if self.field._parent_table.has_parent_table:
            kwargs["related_name"] = to_snake_case(self.field._parent_table["originalID"])
        else:
            kwargs["related_name"] = self._fetch_related_name_for_backward_relations()
        return {**super().field_kwargs, **kwargs}


class M2MRelationMaker(RelationMaker):
    @property
    def field_cls(self):
        return models.ManyToManyField

    @property
    def field_kwargs(self):
        kwargs = {}
        snakecased_fieldname = to_snake_case(self.field.name)
        parent_table = to_snake_case(self.field._parent_table.name)
        kwargs["related_name"] = f"{snakecased_fieldname}_{parent_table}"
        kwargs["through"] = self._make_through_classname(self.dataset.id, self.field.id)
        kwargs["through_fields"] = (parent_table, snakecased_fieldname)

        return {**super().field_kwargs, **kwargs}


class LooseM2MRelationMaker(M2MRelationMaker):
    @property
    def field_cls(self):
        return LooseRelationManyToManyField

    @property
    def field_kwargs(self):
        return {**super().field_kwargs, "relation": self.nm_relation}


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
            relation_maker_cls = RelationMaker.fetch_maker(dataset, field)
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
        # skip nested tables
        if field.is_nested_table:
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
