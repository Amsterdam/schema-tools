# Generated by Django 3.0.2 on 2020-01-29 08:35
from __future__ import annotations

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0001_initial"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="dataset",
            options={
                "ordering": ("ordering", "name"),
                "verbose_name": "Dataset",
                "verbose_name_plural": "Datasets",
            },
        ),
        migrations.CreateModel(
            name="DatasetTable",
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
                ("name", models.CharField(max_length=100)),
                ("enable_geosearch", models.BooleanField(default=True)),
                ("db_table", models.CharField(max_length=100, unique=True)),
                (
                    "display_field",
                    models.CharField(blank=True, max_length=50, null=True),
                ),
                (
                    "geometry_field",
                    models.CharField(blank=True, max_length=50, null=True),
                ),
                (
                    "dataset",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tables",
                        to="datasets.Dataset",
                    ),
                ),
            ],
            options={
                "verbose_name": "Dataset Table",
                "verbose_name_plural": "Dataset Tables",
                "ordering": ("name",),
                "unique_together": {("dataset", "name")},
            },
        ),
    ]
