from __future__ import annotations

import logging
import random
from collections import defaultdict
from typing import Any

from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.models import Dataset
from schematools.types import DatasetTableSchema

logger = logging.getLogger(__name__)


def add_temporal_attrs(
    field_name: str, value: Any, attrs: dict[str, Any], target_table: DatasetTableSchema
):
    """Add extra temporal attributes if target_table is temporal."""
    temporal_attrs = {}
    if temporal := target_table.temporal:
        temporal_identifier = temporal.identifier
        try:
            # The identificatie is one of the element of table.identifier
            identificatie = (set(target_table.identifier) - {temporal_identifier}).pop()
        except KeyError:
            logger.exception(
                "The temporal table `{target_table.name}` should have a composite key."
            )
        for postfix in (identificatie, temporal_identifier):
            try:
                postfix_value = getattr(value, postfix)
            except AttributeError:
                field_info = f"{target_table.name}:{field_name}"
                logger.warning(
                    "Skipping temporal attribute mocking for `%s` `%s`", field_info, postfix
                )
                return attrs
            temporal_attrs[field_name + "_" + postfix] = postfix_value
    return attrs | temporal_attrs


def relate_datasets(*datasets: Dataset) -> None:
    """Add relations to the datasets.

    There is one caveat. Because we are using the Django ORM,
    we do not have models for the extra through tabels for the 1-N models,
    that are added for the BenK related models.
    So these through tables cannot be filled with mock data.
    """
    models = defaultdict(dict)
    for dataset in datasets:
        if not dataset.enable_db:
            logger.warning("Skipping `%s`, `enable_db` is False", dataset.name)
            continue

        factory = DjangoModelFactory(dataset)
        for cls in factory.build_models():
            models[dataset.name][cls._meta.model_name] = cls

    for dataset in datasets:
        dataset_schema = dataset.schema
        for table in dataset_schema.tables:
            model = models[dataset_schema.db_name][
                table.db_name_variant(with_dataset_prefix=False, with_version=False)
            ]
            for f in table.fields:
                field_name = f.python_name

                if f.relation is not None:
                    # We need to get the related_model via the field on the source model
                    # For some reason, getting the related_model from the `models` dict
                    # does not work. The model class seems to be identical,
                    # however the check that Django applies is failing.
                    # Presumably because models are reloaded
                    # (Django warns about this during startup).
                    related_model = model._meta.get_field(field_name).remote_field.model

                    if isinstance(related_model, str):
                        logger.warning(
                            "Model `%s` cannot be resolved, relating will be skipped",
                            related_model,
                        )
                        continue

                    try:
                        values = list(related_model.objects.all())
                        nulled_objects = list(model.objects.all())
                    except ValueError as e:
                        message = str(e)
                        logger.warning("Values cannot be obtained: %s", message)
                        break

                    objs = []
                    if not values:
                        logger.warning("Skipping relate for %s, reason: no values", related_model)
                        continue
                    for obj in nulled_objects:
                        value = random.choice(values)  # noqa: S311 # nosec: B311
                        attrs = {field_name: value}
                        # Add the extra temporal fields that are stored separately
                        # on the object if applicable
                        if not f.is_loose_relation:
                            attrs = add_temporal_attrs(field_name, value, attrs, f.related_table)

                        for attr_name, attr_value in attrs.items():
                            setattr(
                                obj,
                                attr_name,
                                attr_value,
                            )
                        objs.append(obj)

                    if nulled_objects:
                        model.objects.bulk_update(objs, attrs.keys())

                elif f.nm_relation is not None:
                    target_model = model._meta.get_field(field_name).remote_field.model
                    if isinstance(target_model, str):
                        logger.warning(
                            "Skipping relate for %s, reason: model not loaded.", target_model
                        )
                        continue
                    target_values = target_model.objects.all()
                    source_values = model.objects.all()
                    m2m_descriptor = getattr(model, field_name)
                    through_model = m2m_descriptor.through

                    objs = []
                    source_id = table.db_name_variant(
                        with_dataset_prefix=False, with_version=False, postfix="_id"
                    )
                    target_id = field_name + "_id"

                    # first delete all relations
                    through_model.objects.all().delete()
                    for source_value in source_values:
                        target_value = random.choice(target_values)  # noqa: S311, # nosec B311
                        attrs = {source_id: source_value.pk, target_id: target_value.pk}
                        # Add the extra temporal fields that are stored separately
                        # on the object if applicable. This needs to be done for
                        # the source and target side of the relation.
                        if not f.is_loose_relation:
                            attrs = add_temporal_attrs(
                                table.db_name_variant(
                                    with_dataset_prefix=False, with_version=False
                                ),
                                source_value,
                                attrs,
                                table,
                            )
                            attrs = add_temporal_attrs(
                                field_name, target_value, attrs, f.related_table
                            )
                        objs.append(through_model.objects.create(**attrs))

                    if objs:
                        through_model.objects.bulk_update(objs, attrs.keys())
