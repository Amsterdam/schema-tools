from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple, Type
from urllib.parse import urlparse

from django.contrib.gis.db import models
from django.db.models.base import ModelBase
from django.conf import settings

from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    get_db_table_name,
)
from schematools.utils import to_snake_case
from .models import (
    FORMAT_MODELS_LOOKUP,
    JSON_TYPE_TO_DJANGO,
    DynamicModel,
    ObjectMarker,
    LooseRelationField,
    LooseRelationManyToManyField,
)


TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]


class FieldMaker:
    """Generate the field for a JSON-Schema property"""

    def __init__(
        self,
        field_cls: Type[models.Field],
        table: DatasetTableSchema,
        value_getter: Callable[[DatasetSchema], Dict[str, Any]] = None,
        **kwargs,
    ):
        self.field_cls = field_cls
        self.table = table
        self.value_getter = value_getter
        self.kwargs = kwargs
        self.modifiers = [
            getattr(self, an) for an in dir(self) if an.startswith("handle_")
        ]

    def _make_related_classname(self, relation_urn):
        related_dataset, related_table = [
            to_snake_case(part) for part in relation_urn.split(":")[:2]
        ]
        return f"{related_dataset}.{related_table}"

    def _make_through_classname(self, dataset_id, field_name):
        snakecased_fieldname = to_snake_case(field_name)
        through_table_id = get_db_table_name(self.table, snakecased_fieldname)
        # dso-api expects the dataset_id seperated from the table_id by a point
        table_id = "_".join(through_table_id.split("_")[1:])
        dataset_id = through_table_id.split("_")[0]
        return f"{dataset_id}.{table_id}"

    def handle_basic(
        self,
        dataset: DatasetSchema,
        field: DatasetFieldSchema,
        field_cls,
        *args,
        **kwargs,
    ) -> TypeAndSignature:
        kwargs["primary_key"] = field.is_primary
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
        nm_relation = field.nm_relation

        # Short circuit for loose relations, if column is explicitly
        # defined in the relation, this means the value is used as-is to
        # construct a url (no checking on relations)
        if relation:
            relation_parts = relation.split(":")
            _, related_table_name = relation_parts[:2]
            if len(relation_parts) > 2:
                kwargs["db_column"] = f"{to_snake_case(field.name)}_id"
                kwargs["relation"] = relation
                return LooseRelationField, args, kwargs

        if relation is not None or nm_relation is not None:
            assert not (relation and nm_relation)
            field_cls = models.ManyToManyField if nm_relation else models.ForeignKey
            args = [self._make_related_classname(relation or nm_relation)]
            if relation:
                args.append(models.CASCADE if field.required else models.SET_NULL)

            if nm_relation is not None:
                snakecased_fieldname = to_snake_case(field.name)
                parent_table = to_snake_case(field._parent_table.id)
                kwargs["related_name"] = f"{snakecased_fieldname}_{parent_table}"
                # kwargs["db_constraint"] = True
                kwargs["through"] = self._make_through_classname(dataset.id, field.name)
                kwargs["through_fields"] = (parent_table, snakecased_fieldname)
            elif field._parent_table.has_parent_table:
                kwargs["related_name"] = field._parent_table["originalID"]
            else:
                related_name = None
                try:
                    table = dataset.get_table_by_id(related_table_name)
                    for name, relation in table.relations.items():
                        if (
                            relation["table"] == field.table.id
                            and relation["field"] == field.name
                        ):
                            related_name = name
                            break
                except ValueError:
                    pass

                if related_name:
                    kwargs["related_name"] = related_name
                else:
                    kwargs["related_name"] = "+"

            # In schema foreign keys should be specified without _id,
            # but the db_column should be with _id
            if nm_relation is None:
                kwargs["db_column"] = f"{to_snake_case(field.name)}_id"
                kwargs["db_constraint"] = False  # relation is not mandatory

        if nm_relation:
            nm_relation_parts = nm_relation.split(":")
            _, related_table_name = nm_relation_parts[:2]
            if len(nm_relation_parts) > 2:
                field_cls = LooseRelationManyToManyField
                kwargs["relation"] = nm_relation
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
    dataset.add_dataset_to_cache(dataset)
    return [
        model_factory(table=table, base_app_name=base_app_name)
        for table in dataset.get_tables(include_nested=True, include_through=True)
        if tables is None or table.id in tables
    ]


def model_factory(table: DatasetTableSchema, base_app_name=None) -> Type[DynamicModel]:
    """Generate a Django model class from a JSON Schema definition."""
    dataset = table._parent_schema
    app_label = dataset.id
    base_app_name = base_app_name or "dso_api.dynamic_api"
    module_name = f"{base_app_name}.{app_label}.models"
    model_name = to_snake_case(table.id)
    display_field = to_snake_case(table.display_field) if table.display_field else None
    is_temporal = table.is_temporal

    # Generate fields
    fields = {}
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

        try:
            base_class, init_kwargs = JSON_TYPE_TO_DJANGO[type_]
        except KeyError as e:
            raise RuntimeError(
                f"Unable to parse {table.id}: field '{field.name}'"
                f" has unsupported type: {type_}."
            ) from e

        if init_kwargs is None:
            init_kwargs = {}

        # Generate field object
        kls, args, kwargs = FieldMaker(base_class, table, **init_kwargs)(field, dataset)
        if kls is None or kls is ObjectMarker:
            # Some fields are not mapped into classes
            continue

        model_field = kls(*args, **kwargs)

        # Generate name, fix if needed.
        field_name = to_snake_case(field.name)
        model_field.name = field_name
        fields[field_name] = model_field

    # Generate Meta part
    meta_cls = type(
        "Meta",
        (),
        {
            "managed": False,
            "db_table": get_db_table_name(table),
            "app_label": app_label,
            "verbose_name": table.id.title(),
            "ordering": [to_snake_case(fn) for fn in table.identifier],
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
            "_display_field": display_field,
            "_is_temporal": is_temporal,
            "__module__": module_name,
            "Meta": meta_cls,
        },
    )
