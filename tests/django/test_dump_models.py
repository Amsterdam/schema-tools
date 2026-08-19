from __future__ import annotations

from io import StringIO
from types import SimpleNamespace
from typing import cast

import pytest
from django.apps import apps
from django.core.management import call_command
from django.core.validators import RegexValidator
from django.db import models

from schematools.contrib.django.factories import DjangoModelFactory
from schematools.contrib.django.management.commands import dump_models as dump_models_module
from schematools.contrib.django.management.commands.dump_models import Command
from schematools.contrib.django.models import DynamicModel


def sample_formatter_function() -> None:
    pass


@pytest.mark.django_db
def test_dump_models_outputs_dynamic_model_definition(afval_dataset) -> None:
    factory = DjangoModelFactory(afval_dataset)
    built_models = factory.build_models()
    container_model = next(
        model for model in built_models if model._meta.model_name == "containers"
    )
    app_label = container_model._meta.app_label

    stdout = StringIO()
    call_command("dump_models", app_label, stdout=stdout)

    output = stdout.getvalue()
    app_config = apps.get_app_config(app_label)

    assert f"# ---- App: {app_config.verbose_name or app_config.label}" in output
    assert "class containers(DynamicModel):\n" in output
    assert "id = models.CharField(" in output
    assert "primary_key=True" in output
    assert "cluster = models.ForeignKey(" in output
    assert "_display_field = 'id'\n" in output
    assert f"app_label = '{app_label}'\n" in output
    assert f"db_table = '{container_model._meta.db_table}'\n" in output
    assert f"verbose_name = '{container_model._meta.verbose_name}'\n" in output
    assert "def __str__(self):\n        return self.id\n" in output


def test_dump_models_formats_known_callables_and_relations() -> None:
    command = Command(stdout=StringIO())

    field = models.ForeignKey("self", on_delete=models.CASCADE)
    field.name = "parent"

    assert command._format_value(models.CASCADE) == "models.CASCADE"
    assert command._format_value(field) == "models.ForeignKey(on_delete=models.CASCADE, to='self')"
    assert (
        command._get_field_repr(field) == "models.ForeignKey(on_delete=models.CASCADE, to='self')"
    )


def test_dump_models_write_model_for_plain_inherited_model() -> None:
    class DumpModelsParent(models.Model):
        name = models.CharField(max_length=20)

        class Meta:
            app_label = "dump_models_tests"

        def __str__(self) -> str:
            return str(self.name)

    class DumpModelsChild(DumpModelsParent):
        "Child docstring"

        extra = models.IntegerField()

        class Meta:
            app_label = "dump_models_tests"

        def __str__(self) -> str:
            return str(self.extra)

    command = Command(stdout=StringIO())

    command.write_model(DumpModelsChild)

    output = command.stdout.getvalue()
    assert "class DumpModelsChild(DumpModelsParent):\n" in output
    assert '    """Child docstring"""\n\n' in output
    assert "    extra = models.IntegerField()\n" in output
    assert "\n    # Auto created fields:\n" in output
    assert "parent_ptr = models.OneToOneField(" in output
    assert "    class Meta:\n" in output
    assert "_display_field" not in output
    assert "def __str__(self):" not in output


def test_dump_models_writes_wrapped_docstring_and_temporal_attrs() -> None:
    command = Command(stdout=StringIO())
    temporal_model = cast(
        type[DynamicModel],
        type(
            "DumpModelsTemporal",
            (),
            {
                "_dataset": "dataset",
                "_table_schema": "table_schema",
                "_display_field": "name",
                "_is_temporal": True,
            },
        ),
    )

    command.write_docstring("word " * 30)
    command.write_docstring("already\nformatted")
    command.write_dynamic_model_attrs(temporal_model)

    output = command.stdout.getvalue()
    assert '    """word word word' in output
    assert "\n    word" in output
    assert '    """already\nformatted"""\n\n' in output
    assert "    # _dataset = 'dataset'\n" in output
    assert "    # _table_schema = table_schema\n" in output
    assert "    _display_field = 'name'\n" in output
    assert "    _is_temporal = True\n" in output


def test_dump_models_formats_supported_value_types(monkeypatch) -> None:
    command = Command(stdout=StringIO())

    regex_validator = RegexValidator(regex="test")

    def fake_datetime_now() -> None:
        pass

    def fake_date_today() -> None:
        pass

    def fake_timezone_now() -> None:
        pass

    monkeypatch.setattr(dump_models_module, "datetime", SimpleNamespace(now=fake_datetime_now))
    monkeypatch.setattr(dump_models_module, "date", SimpleNamespace(today=fake_date_today))
    monkeypatch.setattr(dump_models_module, "timezone", SimpleNamespace(now=fake_timezone_now))

    assert command._format_value(models.CharField) == "CharField"
    assert command._format_value(regex_validator).startswith(
        "django.core.validators.RegexValidator("
    )
    assert command._format_value([fake_datetime_now, fake_date_today, fake_timezone_now]) == (
        "[datetime.now,date.today,timezone.now]"
    )
    assert command._format_value(models.SET_NULL) == "models.SET_NULL"
    assert command._format_value(models.PROTECT) == "models.DO_NOTHING"
    assert command._format_value(models.SET_DEFAULT) == "models.SET_DEFAULT"
    assert command._format_value(sample_formatter_function) == (
        f"{sample_formatter_function.__module__}.{sample_formatter_function.__qualname__}"
    )
    assert command._format_value(len).startswith("<built-in function")
    assert command._format_value("fallback") == "'fallback'"
