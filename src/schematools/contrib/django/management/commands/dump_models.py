from __future__ import annotations

import inspect
import textwrap
from argparse import ArgumentParser
from datetime import date, datetime
from typing import Any

from django.apps import AppConfig, apps
from django.core.management import BaseCommand
from django.db import models
from django.utils import timezone
from django.utils.functional import partition

from schematools.contrib.django.models import DynamicModel


class Command(BaseCommand):
    """Dump the dynamically generated models."""

    help = "Dump the (dynamic) model definitions that Django holds in-memory."  # noqa: A003

    model_meta_args: list[tuple[str, Any]] = [
        # All possible options in Meta, with their defaults.
        # https://docs.djangoproject.com/en/3.2/ref/models/options/
        # The original model.Meta or Options.meta is not available after construction,
        # so will have to fill out the defaults to generate a reasonable representation.
        ("abstract", False),
        ("app_label", None),
        ("base_manager_name", None),
        ("constraints", []),
        ("db_table", None),
        ("default_manager_name", None),
        ("default_related_name", None),
        ("get_latest_by", None),
        ("index_together", ()),
        ("indexes", []),
        ("managed", True),
        ("order_with_respect_to", None),
        ("ordering", ["id"]),
        ("permissions", []),
        ("proxy", False),
        ("unique_together", ()),
        ("verbose_name", ""),  # is modelname
        ("verbose_name_plural", ""),  # is {modelname}s by default
    ]

    path_aliases: list[tuple[str, str]] = [
        ("django.db.models.", "models."),
        ("django.contrib.gis.db.models.fields.", ""),
        ("schematools.contrib.django.fields.", ""),
    ]

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Hook to add arguments."""
        parser.add_argument(
            "args", metavar="app_label", nargs="*", help="Names of Django apps to dump"
        )

    def handle(self, *args: str, **options: Any) -> None:
        """Main function of this command."""
        app_labels = sorted(args or apps.app_configs.keys())

        for app_label in app_labels:
            app: AppConfig = apps.get_app_config(app_label)
            self.write_header(app)
            models = app.get_models(include_auto_created=True)
            for model in sorted(models, key=lambda m: m._meta.model_name):
                self.write_model(model)

    def write_header(self, app: AppConfig) -> None:
        """Write app start header."""
        self.stdout.write(f"# ---- App: {app.verbose_name or app.label}\n\n\n")

    def write_model(self, model: type[models.Model]) -> None:
        """Write the representation of a complete Django model to the output."""
        bases = ", ".join(base_class.__name__ for base_class in model.__bases__)
        self.stdout.write(f"class {model.__name__}({bases}):\n")

        if model.__doc__:
            self.write_docstring(model.__doc__)

        # Get model fields, split in auto created vs declared fields
        all_fields = model._meta.get_fields(include_parents=False, include_hidden=True)
        declared_fields, auto_created_fields = partition(lambda f: f.auto_created, all_fields)
        for field in declared_fields:
            self.write_field(field)

        if auto_created_fields:
            self.stdout.write("\n    # Auto created fields:\n")
            for field in sorted(auto_created_fields, key=lambda f: f.name):
                if isinstance(field, models.ForeignObjectRel):
                    self.write_reverse_field(field)
                else:
                    self.write_field(field)

        self.stdout.write("\n")

        if issubclass(model, DynamicModel):
            self.write_dynamic_model_attrs(model)

        self.write_model_meta(model)

        if issubclass(model, DynamicModel) and model._display_field:
            self.write_model_str(model)

        self.stdout.write("\n\n")

    def write_docstring(self, doc: str) -> None:
        """Write the docstring for a class."""
        if "\n" not in doc:  # if not formatted already
            # Wrap our description text
            doc = textwrap.fill(doc, 96, subsequent_indent="    ")
            if "\n" in doc:  # if wrapped
                doc += "\n"
        self.stdout.write(f'    """{doc}"""\n\n')

    def write_dynamic_model_attrs(self, model: type[DynamicModel]) -> None:
        """Write the attributes defined by DjangoModelFactory.build_model()for completeness."""
        self.stdout.write("    # Set by DjangoModelFactory.build_model()\n")
        self.stdout.write(f"    # _dataset = {model._dataset!r}\n")
        self.stdout.write(f"    # _table_schema = {model._table_schema}\n")
        self.stdout.write(f"    _display_field = {model._display_field!r}\n")
        if model._is_temporal:
            self.stdout.write("    _is_temporal = True\n")
        self.stdout.write("\n")

    def write_model_meta(self, model: type[models.Model]) -> None:
        """Write the 'class Meta' section."""
        self.stdout.write("    class Meta:\n")
        for meta_arg, default in self.model_meta_args:
            value = getattr(model._meta, meta_arg, ...)
            if value != default:
                self.stdout.write(f"        {meta_arg} = {value!r}\n")

    def write_model_str(self, model: type[DynamicModel]) -> None:
        """For dynamic model, we know how __str__() looks like."""
        self.stdout.write("\n")
        self.stdout.write("    def __str__(self):\n")
        self.stdout.write(f"        return self.{model._display_field}\n")

    def write_field(self, field: models.Field) -> None:
        """Write how a field would have been written in a models file."""
        # Note that migration files use the 'name' from field.deconstruct()
        # but this is always identical to 'field.name' for standard Django fields.
        self.stdout.write(f"    {field.name} = {self._get_field_repr(field)}\n")

    def _get_field_repr(self, field: models.Field) -> str:
        """Generate the field representation, it's reused for arguments."""
        name, path, args, kwargs = field.deconstruct()
        return self._get_deconstructable_repr(path, args, kwargs)

    def _get_deconstructable_repr(self, path: str, args: list[Any], kwargs: dict[str, Any]) -> str:
        for prefix, alias in self.path_aliases:
            if path.startswith(prefix):
                path = alias + path[len(prefix) :]

        str_args = ", ".join(self._format_value(arg) for arg in args)
        str_kwargs = ", ".join(f"{n}={self._format_value(v)}" for n, v in kwargs.items())
        return f"{path}({str_args}{str_kwargs})"

    def write_reverse_field(self, field: models.ForeignObjectRel) -> None:
        """Mention the reverse relations for clarity."""
        source_field = field.remote_field

        cls_name = field.__class__.__name__
        related_model = _format_model_name(field.related_model)
        source_name = f"<{source_field.__class__.__name__} at {related_model}.{source_field.name}>"

        comment = "hidden reverse relation" if field.is_hidden() else "reverse relation"
        self.stdout.write(
            f"    # {comment}: {field.name} = {cls_name}(field={source_name}, ...)\n"
        )

    def _format_value(self, value: Any) -> str:
        """Format the model kwarg, some callables should be mapped to their code name."""
        if isinstance(value, models.Field):
            # e.g. ArrayField(base_field=models.CharField(...))
            return self._get_field_repr(value)
        elif isinstance(value, type):
            return value.__name__
        elif isinstance(value, object) and hasattr(value, "deconstruct"):
            # A @deconstructible, e.g. URLPathValidator.
            return self._get_deconstructable_repr(*value.deconstruct())
        elif isinstance(value, list):
            # for validators=[...]
            return "[{}]".format(",".join(self._format_value(v) for v in value))
        elif callable(value):
            if value is datetime.now:
                return "datetime.now"
            elif value is date.today:
                return "date.today"
            elif value is timezone.now:
                return "timezone.now"
            elif value is models.CASCADE:
                return "models.CASCADE"
            elif value is models.SET_NULL:
                return "models.SET_NULL"
            elif value is models.PROTECT:
                return "models.DO_NOTHING"
            elif value is models.SET_DEFAULT:
                return "models.SET_DEFAULT"
            elif value is models.PROTECT:
                return "models.PROTECT"
            elif inspect.isfunction(value):
                # e.g. validators=[some_function]
                return f"{value.__module__}.{value.__qualname__}"

        return repr(value)


def _format_model_name(model: type[models.Model]) -> str:
    return f"{model._meta.app_label}.{model._meta.model_name}"
