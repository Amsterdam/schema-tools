from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0025_remove_datasetversion_lifecycle_status_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="datasetversion",
            name="status",
            field=models.CharField(
                choices=[
                    ("D", "under_development"),
                    ("S", "stable"),
                    ("U", "superseded"),
                    ("X", "deprecated"),
                ],
                default="D",
            ),
        ),
    ]
