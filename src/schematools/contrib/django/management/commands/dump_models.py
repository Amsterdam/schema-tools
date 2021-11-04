import textwrap
from argparse import ArgumentParser
from datetime import date, datetime
from typing import Any, List, Tuple, Type

from django.apps import AppConfig, apps
from django.core.management import BaseCommand
from django.db import models
from django.utils import timezone
from django.utils.functional import partition

from schematools.contrib.django.models import DynamicModel


class Command(BaseCommand):
    """Dump the dynamically generated models"""

    help = "Dump the (dynamic) model definitions that Django holds in-memory."

    model_meta_args: List[Tuple[str, Any]] = [
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

    path_aliases: List[Tuple[str, str]] = [
        ("django.db.models.", "models."),
        ("django.contrib.gis.db.models.fields.", ""),
        ("django_postgres_unlimited_varchar.", ""),
    ]

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Hook to add arguments"""
        parser.add_argument(
            "args", metavar="app_label", nargs="*", help="Names of Django apps to dump"
        )

    def handle(self, *args: str, **options: Any) -> None:
        """Main function of this command"""
        app_labels = sorted(args or apps.app_configs.keys())

        for app_label in app_labels:
            app: AppConfig = apps.get_app_config(app_label)
            self.write_header(app)
            models = app.get_models(include_auto_created=True)
            for model in sorted(models, key=lambda m: m._meta.model_name):
                self.write_model(model)

    def write_header(self, app: AppConfig) -> None:
        self.stdout.write(f"# ---- App: {app.verbose_name or app.label}\n\n\n")

    def write_model(self, model: Type[models.Model]) -> None:
        bases = ", ".join(base_class.__name__ for base_class in model.__bases__)
        self.stdout.write(f"class {model.__name__}({bases}):\n")

        if model.__doc__:
            doc = textwrap.fill(model.__doc__, 96, subsequent_indent="    ")
            if "\n" in doc:
                doc += "\n"
            self.stdout.write(f'    """{doc}"""\n\n')

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

        self.stdout.write("    class Meta:\n")
        for meta_arg, default in self.model_meta_args:
            value = getattr(model._meta, meta_arg, ...)
            if value != default:
                self.stdout.write(f"        {meta_arg} = {value!r}\n")

        if issubclass(model, DynamicModel) and model._display_field:
            self.write_model_str(model)

        self.stdout.write("\n\n")

    def write_dynamic_model_attrs(self, model: Type[DynamicModel]) -> None:
        """Write the attributes defined by model_factory() for completeness"""
        self.stdout.write(f"    # Set by model_factory()\n")
        self.stdout.write(f"    # _dataset = {model._dataset!r}\n")
        self.stdout.write(f"    # _table_schema = {model._table_schema}\n")
        self.stdout.write(f"    _display_field = {model._display_field!r}\n")
        if model._is_temporal:
            self.stdout.write("    _is_temporal = True\n")
        self.stdout.write("\n")

    def write_model_str(self, model: Type[DynamicModel]) -> None:
        """For dynamic model, we know how __str__() looks like."""
        self.stdout.write("\n")
        self.stdout.write("    def __str__(self):\n")
        self.stdout.write(f"        return self.{model._display_field}\n")

    def write_field(self, field: models.Field) -> None:
        """Write how a field would have been written in a models file."""
        name, path, args, kwargs = field.deconstruct()
        for prefix, alias in self.path_aliases:
            if path.startswith(prefix):
                path = alias + path[len(prefix) :]

        str_args = ", ".join(_format_value(arg) for arg in args)
        str_kwargs = ", ".join(f"{n}={_format_value(v)}" for n, v in kwargs.items())
        self.stdout.write(f"    {name} = {path}({str_args}{str_kwargs})\n")

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


def _format_value(value: Any) -> str:
    """Format the model kwarg, some callables should be mapped to their code name."""
    if callable(value):
        if value is datetime.now:
            return "datetime.now"
        if value is date.today:
            return "date.today"
        if value is timezone.now:
            return "timezone.now"
        if value is models.CASCADE:
            return "models.CASCADE"
        if value is models.SET_NULL:
            return "models.SET_NULL"
        if value is models.PROTECT:
            return "models.DO_NOTHING"
        if value is models.SET_DEFAULT:
            return "models.SET_DEFAULT"
        if value is models.PROTECT:
            return "models.PROTECT"

    return repr(value)


def _format_model_name(model: Type[models.Model]) -> str:
    return f"{model._meta.app_label}.{model._meta.model_name}"