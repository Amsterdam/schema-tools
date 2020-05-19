from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple, Type
from urllib.parse import urlparse

from django.contrib.gis.db import models
from django.db.models.base import ModelBase
from django.conf import settings
from string_utils import slugify

from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    is_possible_display_field,
    get_db_table_name,
)
from .models import (
    DATE_MODELS_LOOKUP,
    JSON_TYPE_TO_DJANGO,
    DynamicModel,
)


TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]


class FieldMaker:
    """Generate the field for a JSON-Schema property"""

    def __init__(
        self,
        field_cls: Type[models.Field],
        value_getter: Callable[[DatasetSchema], Dict[str, Any]] = None,
        **kwargs,
    ):
        self.field_cls = field_cls
        self.value_getter = value_getter
        self.kwargs = kwargs
        self.modifiers = [
            getattr(self, an) for an in dir(self) if an.startswith("handle_")
        ]

    def _make_related_classname(self, relation_urn):
        dataset_name, table_name = relation_urn.split(":")
        return f"{dataset_name}.{table_name.capitalize()}"

    def handle_basic(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        kwargs["primary_key"] = field.is_primary
        if not field.is_primary:
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
        if field.data.get("type", "").lower() == "array":
            base_field, _ = JSON_TYPE_TO_DJANGO[
                field.data.get("entity", {}).get("type", "string")
            ]
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
        relation = field.relation

        if relation is not None:
            field_cls = models.ForeignKey
            on_delete = models.CASCADE if field.required else models.SET_NULL
            args = [self._make_related_classname(relation), on_delete]

            if field._parent_table.has_parent_table:
                kwargs["related_name"] = field._parent_table["originalID"]

            # In schema foreign keys should be specified without _id,
            # but the db_column should be with _id
            kwargs["db_column"] = f"{slugify(field.name, separator='_')}_id"
            kwargs["db_constraint"] = False  # don't expect relations to exist.
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
            field_cls = DATE_MODELS_LOOKUP[format_]
        return field_cls, args, kwargs

    def __call__(
        self, field: DatasetFieldSchema, dataset: DatasetSchema
    ) -> TypeAndSignature:
        field_cls = self.field_cls
        kwargs = self.kwargs
        args = []

        for modifier in self.modifiers:
            field_cls, args, kwargs = modifier(
                dataset, field, field_cls, *args, **kwargs
            )

        return field_cls, args, kwargs


def schema_models_factory(
    dataset: DatasetSchema, tables=None, base_app_name=None
) -> List[Type[DynamicModel]]:
    """Generate Django models from the data of the schema."""
    return [
        model_factory(table=table, base_app_name=base_app_name)
        for table in dataset.get_tables(include_nested=True)
        if tables is None or table.id in tables
    ]


def model_factory(table: DatasetTableSchema, base_app_name=None) -> Type[DynamicModel]:
    """Generate a Django model class from a JSON Schema definition."""
    dataset = table._parent_schema
    app_label = dataset.id
    base_app_name = base_app_name or "dso_api.dynamic_api"
    module_name = f"{base_app_name}.{app_label}.models"
    model_name = f"{table.id.capitalize()}"

    # Generate fields
    fields = {}
    display_field = None
    for field in table.fields:
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
        base_class, init_kwargs = JSON_TYPE_TO_DJANGO[type_]
        if init_kwargs is None:
            init_kwargs = {}

        # Generate field object
        kls, args, kwargs = FieldMaker(base_class, **init_kwargs)(field, dataset)
        if kls is None:
            # Some fields are not mapped into classes
            continue
        model_field = kls(*args, **kwargs)

        # Generate name, fix if needed.
        field_name = slugify(field.name, separator="_")
        model_field.name = field_name
        fields[field_name] = model_field

        if not display_field and is_possible_display_field(field):
            display_field = field.name

    # Generate Meta part
    meta_cls = type(
        "Meta",
        (),
        {
            "managed": False,
            "db_table": get_db_table_name(table),
            "app_label": app_label,
            "verbose_name": table.id.title(),
            "ordering": ("id",),
        },
    )

    # Generate the model
    return ModelBase(
        model_name,
        (DynamicModel,),
        {
            **fields,
            "_dataset_schema": dataset,
            "_table_schema": table,
            "_display_field": "",
            "__module__": module_name,
            "Meta": meta_cls,
        },
    )
