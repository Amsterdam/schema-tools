# Generated by Django 3.1.12 on 2021-07-20 11:09
from __future__ import annotations

from django.db import migrations, models

import schematools.contrib.django.validators


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0011_auto_20210623_1135"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dataset",
            name="schema_data",
            field=models.TextField(
                validators=[schematools.contrib.django.validators.validate_json],
                verbose_name="Amsterdam Schema Contents",
            ),
        ),
    ]
