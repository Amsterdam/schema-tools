# Generated by Django 3.2.20 on 2023-09-12 11:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0015_alter_dataset_id_alter_datasetfield_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="dataset",
            name="view_data",
            field=models.TextField(blank=True, null=True, verbose_name="View SQL"),
        ),
    ]
