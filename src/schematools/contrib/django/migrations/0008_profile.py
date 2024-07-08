# Generated by Django 3.0.7 on 2020-09-21 14:54
from __future__ import annotations

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0007_datasettable_is_temporal"),
    ]

    operations = [
        migrations.CreateModel(
            name="Profile",
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
                ("scopes", models.CharField(max_length=255)),
                (
                    "schema_data",
                    django.contrib.postgres.fields.jsonb.JSONField(
                        verbose_name="Amsterdam Schema Contents"
                    ),
                ),
            ],
        ),
    ]
