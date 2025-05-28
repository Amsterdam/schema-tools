from __future__ import annotations

import json

from django.db import migrations

from schematools import DEFAULT_SCHEMA_URL
from schematools.loaders import get_schema_loader
from schematools.types import DatasetSchema


def fill_schema_data(apps, schema_editor):
    Dataset = apps.get_model("datasets", "Dataset")
    loader = get_schema_loader(DEFAULT_SCHEMA_URL)
    for dataset in Dataset.objects.all():
        schema = DatasetSchema.from_dict(
            json.loads(dataset.schema_data),
            loader=loader,
        )
        dataset.schema_data = schema.json(
            inline_tables=True, inline_publishers=True, inline_scopes=True
        )
        dataset.save()


class Migration(migrations.Migration):
    dependencies = [
        ("datasets", "0021_remove_dataset_is_default_version_and_more"),
    ]

    operations = [migrations.RunPython(fill_schema_data, reverse_code=migrations.RunPython.noop)]
