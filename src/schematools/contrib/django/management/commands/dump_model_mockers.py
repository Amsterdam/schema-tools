from __future__ import annotations

from argparse import ArgumentParser
from typing import Any

from django.apps import AppConfig, apps
from django.core.management import BaseCommand

from schematools.contrib.django.models import DynamicModel


class Command(BaseCommand):
    """Dump the dynamically generated ModelFactories."""

    help = "Dump the (dynamic) ModelMocker definitions that Django holds in-memory."  # noqa: A003

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

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Hook to add arguments."""
        parser.add_argument(
            "args",
            metavar="app_label",
            nargs="*",
            help="Names of Django apps to dump the ModelFactory for",
        )

    def handle(self, *args: str, **options: Any) -> None:
        """Main function of this command."""
        app_labels = sorted(args or apps.app_configs.keys())

        for app_label in app_labels:
            app: AppConfig = apps.get_app_config(app_label)
            self.write_header(app)
            models = app.get_models(include_auto_created=True)
            for model in sorted(models, key=lambda m: m._meta.model_name):
                # print(model)
                # <class 'dso_api.dynamic_api.aardgasvrijezones.models.buurt'>
                # <class 'dso_api.dynamic_api.aardgasvrijezones.models.buurtinitiatief'>
                self.write_model_mocker(model)

    def write_header(self, app: AppConfig) -> None:
        """Write app start header."""
        self.stdout.write(f"# ---- App: {app.verbose_name or app.label}\n\n\n")

    def write_model_mocker(self, model: type[DynamicModel]) -> None:
        """Write the representation of a complete DjangoModelFactory
        (ModelMocker in our terminology) to the output.
        """
        bases = ", ".join(base_class.__name__ for base_class in model.__bases__)
        self.stdout.write(f"class {model.__name__}({bases}):\n")

        for field in model._meta.get_fields():
            print(repr(field))
        # <django.db.models.fields.BigIntegerField: id>
        # <django.contrib.gis.db.models.fields.GeometryField: geometry>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: buurt_code>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: buurt_naam>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: toelichting>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: aandeel_kookgas>
        # class buurtinitiatief(DynamicModel):
        # <django.db.models.fields.BigIntegerField: id>
        # <django.contrib.gis.db.models.fields.GeometryField: geometry>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: buurtinitiatief_type>
        # <django_postgres_unlimited_varchar.UnlimitedCharField: buurt_naam>
        # <django.db.models.fields.FloatField: x_coordinaat>
        # <django.db.models.fields.FloatField: y_coordinaat>
