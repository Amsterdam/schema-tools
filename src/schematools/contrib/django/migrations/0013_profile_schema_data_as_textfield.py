# Generated by Django 3.1.12 on 2021-07-20 12:12

from django.db import migrations, models

import schematools.contrib.django.validators


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0012_schema_data_as_textfield"),
    ]

    operations = [
        migrations.AlterField(
            model_name="profile",
            name="schema_data",
            field=models.TextField(
                validators=[schematools.contrib.django.validators.validate_json],
                verbose_name="Amsterdam Schema Contents",
            ),
        ),
    ]
