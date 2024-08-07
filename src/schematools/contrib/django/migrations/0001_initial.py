# Generated by Django 3.0.2 on 2020-01-23 11:13
from __future__ import annotations

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Dataset",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "name",
                    models.CharField(max_length=50, unique=True, verbose_name="Name"),
                ),
                ("ordering", models.IntegerField(default=1, verbose_name="Ordering")),
                ("enable_api", models.BooleanField(default=True)),
                (
                    "schema_data",
                    django.contrib.postgres.fields.jsonb.JSONField(
                        verbose_name="Amsterdam Schema Contents"
                    ),
                ),
            ],
            options={
                "verbose_name": "Dataset",
                "verbose_name_plural": "Datasets",
            },
        ),
    ]
