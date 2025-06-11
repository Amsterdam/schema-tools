"""The Django models for Amsterdam Schema data.

When models are generated with
:func:`~schematools.contrib.django.factories.DjangoModelFactory.build_model()`,
they all inherit from :class:`~schematools.contrib.django.models.DynamicModel` to have
a common interface.

When the schema data is imported, the models
:class:`~schematools.contrib.django.models.Dataset`,
:class:`~schematools.contrib.django.models.DatasetTable`,
:class:`~schematools.contrib.django.models.DatasetField` and
:class:`~schematools.contrib.django.models.Profile` are all filled.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TypeVar

from django.conf import settings
from django.db import models, transaction
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from schematools.exceptions import DatasetFieldNotFound
from schematools.loaders import CachedSchemaLoader
from schematools.naming import to_snake_case
from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    DatasetVersionSchema,
    ProfileSchema,
    SemVer,
)

from . import managers
from .validators import URLPathValidator, validate_json

logger = logging.getLogger(__name__)


class DynamicModel(models.Model):
    """Base class to tag and detect dynamically generated models."""

    #: Overwritten by subclasses / factory
    CREATION_COUNTER = None
    _dataset: Dataset = None  # type: ignore[assignment]
    _dataset_schema: DatasetSchema = None  # type: ignore[assignment]
    _table_schema: DatasetTableSchema = None  # type: ignore[assignment]
    _display_field = None
    _is_temporal = None

    class Meta:
        abstract = True

    def __str__(self) -> str:
        if self._display_field:
            return str(getattr(self, self._display_field))
        else:
            # this will not be shown in the dso-api view
            # and will be omitted when display field is empty or not present
            return f"(no title: {self._meta.object_name} #{self.pk})"

    # These classmethods could have been a 'classproperty',
    # but this ensures the names don't conflict with fields from the schema.
    @classmethod
    def get_dataset(cls) -> Dataset:
        """Give access to the original dataset that this models is part of."""
        return cls._dataset

    @classmethod
    def get_dataset_id(cls) -> str:
        """Give access to the original dataset ID that this model is part of."""
        return cls._dataset_schema.id

    @classmethod
    def get_dataset_path(cls) -> str:
        """Give access to the api path this dataset should be published on."""
        return cls._dataset.path

    @classmethod
    def get_dataset_schema(cls) -> DatasetSchema:
        """Give access to the original dataset schema that this model is a part of."""
        return cls._dataset_schema

    @classmethod
    def table_schema(cls) -> DatasetTableSchema:
        """Give access to the original table_schema that this model implements."""
        return cls._table_schema

    @classmethod
    def get_field_schema(
        cls, model_field: models.Field | models.ForeignObjectRel
    ) -> DatasetFieldSchema:
        """Provide access to the underlying amsterdam schema field that created the model field."""
        if isinstance(model_field, models.ForeignObjectRel):
            # When Django auto-creates the related field, it doesn't have `field_schema`,
            # but it can be resolved by looking up the original forward relation.
            model_field = model_field.remote_field

        try:
            # This internal property is assigned by DjangoModelFactory.build_model()
            return model_field.field_schema
        except AttributeError:
            # Easier to ask for forgiveness than permission;
            if not issubclass(model_field.model, DynamicModel):
                raise ValueError(
                    "get_field_schema() is only usable on fields from on DynamicModel instances."
                ) from None

            if model_field.auto_created:
                raise ValueError(
                    "get_field_schema() can't be used on"
                    f" '{model_field.model._meta.model_name}.{model_field.name}',"
                    f" because that is an auto-created field."
                ) from None
            raise

    @classmethod
    def get_table_id(cls) -> str:
        """Give access to the table name"""
        return cls._table_schema.id

    @classmethod
    def has_parent_table(cls) -> bool:
        """Check if table is sub table for another table."""
        return cls._table_schema.has_parent_table

    @classmethod
    def has_display_field(cls) -> bool:
        """Tell whether a display field is configured."""
        return cls._display_field is not None

    @classmethod
    def get_display_field(cls) -> str | None:
        """Return the name of the display field, for usage by Django models."""
        return cls._display_field

    @classmethod
    def is_temporal(cls) -> bool:
        """Indicates if this model has temporary characteristics."""
        return cls._is_temporal


M = TypeVar("M", bound=DynamicModel)


class Dataset(models.Model):
    """A registry of all available datasets that are uploaded in the API server.

    Each model holds the contents of an "Amsterdam Schema",
    that contains multiple tables.
    """

    name = models.CharField(_("Name"), unique=True, max_length=50)
    schema_data = models.TextField(_("Amsterdam Schema Contents"), validators=[validate_json])
    view_data = models.TextField(_("View SQL"), blank=True, null=True)
    default_version = models.CharField(_("Default version"), default="v1")

    # Settings for publishing the schema:
    enable_api = models.BooleanField(default=True)
    enable_db = models.BooleanField(default=True)
    enable_export = models.BooleanField(default=False)
    path = models.TextField(unique=True, blank=False, validators=[URLPathValidator()])
    auth = models.CharField(_("Authorization"), blank=True, null=True, max_length=250)
    ordering = models.IntegerField(_("Ordering"), default=1)

    objects = managers.DatasetQuerySet.as_manager()

    class Meta:
        ordering = ("ordering", "name")
        verbose_name = _("Dataset")
        verbose_name_plural = _("Datasets")

    def __str__(self):
        return self.name

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loader = None

        # The check makes sure that deferred fields are not checked for changes,
        # nor that creating the model
        self._old_schema_data = (
            self.schema_data if "schema_data" in self.__dict__ and not self._state.adding else None
        )

    def save(self, *args, **kwargs):
        """Perform a final data validation check, and additional updates."""
        if self.schema_data_changed() and (self.schema_data or not self._state.adding):
            self.__dict__.pop("schema", None)  # clear cached property
            # The extra "and" above avoids the transaction savepoint for an empty dataset.
            # Ensure both changes are saved together
            with transaction.atomic():
                super().save(*args, **kwargs)
                self.save_schema_tables()
        else:
            super().save(*args, **kwargs)

    save.alters_data = True

    @classmethod
    def name_from_schema(cls, schema: DatasetSchema) -> str:
        """Generate dataset name from schema"""
        return to_snake_case(schema.id)

    @classmethod
    def create_for_schema(
        cls,
        schema: DatasetSchema,
        path: str | None = None,
        enable_db: bool = True,
        save: bool = True,
    ) -> Dataset:
        """Create the schema based on the Amsterdam Schema JSON input"""
        name = cls.name_from_schema(schema)
        if path is None:
            path = name
        obj = cls(
            name=name,
            schema_data=schema.json(
                inline_tables=True, inline_publishers=True, inline_scopes=True
            ),
            view_data=schema.get_view_sql(),
            auth=" ".join(schema.auth),
            path=path,
            default_version=schema.default_version,
            enable_api=schema.has_an_available_version,
            enable_db=enable_db,
        )
        obj._loader = schema.loader  # retain collection on saving
        if save:
            obj.save()
        obj.__dict__["schema"] = schema  # Avoid serializing/deserializing the schema data
        return obj

    def save_for_schema(self, schema: DatasetSchema, path: str, save: bool = True) -> bool:
        """Update this model with schema data"""
        self.schema_data = schema.json(
            inline_tables=True, inline_publishers=True, inline_scopes=True
        )
        self.view_data = schema.get_view_sql()
        self.auth = " ".join(schema.auth)
        self.default_version = schema.default_version
        self.enable_api = schema.has_an_available_version

        changed = self.schema_data_changed() or self.path != path

        self.path = path
        self._loader = schema.loader  # retain collection on saving

        if changed and save:
            self.save(
                update_fields=[
                    "schema_data",
                    "view_data",
                    "auth",
                    "default_version",
                    "enable_api",
                    "path",
                ]
            )

        self.__dict__["schema"] = schema  # Avoid serializing/deserializing the schema data
        return changed

    def save_schema_tables(self):
        """Expose the schema data to the DatasetTable.
        This allows other projects (e.g. geosearch) to process our dynamic tables.
        """
        if not self.schema_data:
            # no schema stored -> no tables
            if self._old_schema_data:
                self.tables.all().delete()
                self.versions.all().delete()
            return

        new_definitions = {t.db_name: t for t in self.schema.get_all_tables(include_nested=True)}
        new_names = set(new_definitions.keys())
        existing_models = {t.db_table: t for t in self.tables.all()}
        existing_names = set(existing_models.keys())

        # Create models for newly added tables
        for added_name in new_names - existing_names:
            table = new_definitions[added_name]
            existing_models[added_name] = DatasetTable.create_for_schema(self, table)

        # Update models for updated tables
        for changed_name in existing_names & new_names:
            table = new_definitions[changed_name]
            existing_models[changed_name].save_for_schema(table)

        # Remove tables that are no longer part of the schema.
        for removed_name in existing_names - new_names:
            existing_models[removed_name].delete()

        # Create/update versions
        for vmajor, version in self.schema.versions.items():
            try:
                version_instance = DatasetVersion.objects.get(dataset=self, version=vmajor)
            except DatasetVersion.DoesNotExist:
                version_instance = DatasetVersion.create_for_schema(version, dataset=self)
            else:
                version_instance.lifecycle_status = DatasetVersion.LifecycleStatus[
                    version.lifecycle_status.value.upper()
                ]
                version_instance.save()
                # Remove tables that are no longer in the version.
                for table in version_instance.tables.all():
                    if table.db_table not in existing_models:
                        version_instance.tables.remove(table)

            # Add all tables to the version (duplicates are ignored).
            version_instance.tables.add(
                *(existing_models[table.db_name] for table in version.tables)
            )

    save_schema_tables.alters_data = True

    @cached_property
    def schema(self) -> DatasetSchema:
        """Provide access to the schema data."""
        # The _loader value is filled by the queryset,
        # so any object that is fetched by same the queryset uses the same shared cache.
        return self.get_schema(loader=self._loader)

    def get_schema(self, loader: CachedSchemaLoader) -> DatasetSchema:
        """Extract the schema data from this model, and connect it with a dataset collection."""
        if not self.schema_data:
            raise RuntimeError("Dataset.schema_data is empty")

        return DatasetSchema.from_dict(
            json.loads(self.schema_data),
            loader=loader,
        )

    @cached_property
    def has_geometry_fields(self) -> bool:
        return any(table.has_geometry_fields for table in self.schema.tables)

    def schema_data_changed(self):
        """Check whether the schema_data attribute changed"""
        return (
            "schema_data" in self.__dict__  # this checks for deferred attributes
            and self.schema_data != self._old_schema_data
        )

    def create_models(
        self,
        base_app_name: str | None = None,
        base_model: type[M] = DynamicModel,
        include_versioned_tables: bool = False,
    ) -> list[type[M]]:
        """Extract the models found in the schema"""
        if not self.enable_db:
            return []
        else:
            # Prevent circular import
            from schematools.contrib.django.factories import DjangoModelFactory

            factory = DjangoModelFactory(
                self,
                base_app_name=base_app_name,
                base_model=base_model,
            )
            return factory.build_models()


class DatasetVersion(models.Model):
    class LifecycleStatus(models.TextChoices):
        EXPERIMENTAL = "E", "experimental"
        STABLE = "S", "stable"

    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="versions")
    version = models.CharField(default="v1", max_length=3)
    lifecycle_status = models.CharField(
        choices=LifecycleStatus.choices,
        default=LifecycleStatus.EXPERIMENTAL,
    )

    def __str__(self):
        return f"{self.dataset.name}_{self.version}"

    @classmethod
    def create_for_schema(cls, version_schema: DatasetVersionSchema, dataset: Dataset | None):
        if dataset is None:
            try:
                dataset = Dataset.objects.get(name=to_snake_case(version_schema.schema.id))
            except Dataset.DoesNotExist as e:
                raise RuntimeError(
                    f"Dataset '{to_snake_case(version_schema.schema.id)}' not found!"
                ) from e
        obj = cls(
            dataset=dataset,
            version=version_schema.version,
            lifecycle_status=cls.LifecycleStatus[version_schema.lifecycle_status.value.upper()],
        )
        obj.save()
        return obj


class DatasetTable(models.Model):
    """Exposed metadata per schema.

    This table can be read by the 'geosearch' project to locate all our tables and data sources.
    """

    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="tables")
    dataset_versions = models.ManyToManyField(DatasetVersion, related_name="tables")
    name = models.CharField(max_length=100)
    version = models.TextField(default=SemVer("1.0.0"))

    # Exposed metadata from the jsonschema, so other apps (e.g. geosearch) can query these
    auth = models.CharField(max_length=250, blank=True, null=True)
    enable_export = models.BooleanField(default=False)
    enable_geosearch = models.BooleanField(default=True)
    db_table = models.CharField(max_length=100, unique=True)
    display_field = models.CharField(max_length=50, null=True, blank=True)
    geometry_field = models.CharField(max_length=50, null=True, blank=True)
    geometry_field_type = models.CharField(max_length=50, null=True, blank=True)
    is_temporal = models.BooleanField(null=False, blank=False, default=False)
    id_field = models.CharField(max_length=50, blank=False, default="id")

    class Meta:
        ordering = ("name",)
        verbose_name = _("Dataset Table")
        verbose_name_plural = _("Dataset Tables")
        unique_together = [
            ("dataset", "name", "version"),
        ]

    def __str__(self):
        return self.name

    @classmethod
    def _get_geometry_field(cls, table_schema):
        try:
            field = table_schema.main_geometry_field
        except DatasetFieldNotFound:
            # fallback to first GeoJSON field as geometry field.
            field = next((f for f in table_schema.fields if f.is_geo), None)
            if field is None:
                return None, None

        match = re.search(r"schema\/(?P<schema>\w+)\.json", field.type)
        geo_type = match.group("schema") if match is not None else None
        return field.db_name, geo_type

    @classmethod
    def _get_id_field(cls, table_schema):
        """Gets the id_field, this is the PK that is needed by Django and geosearch.

        An `id` field will be added to the schema for tables with a compound key.
        """
        identifier = table_schema.identifier
        return to_snake_case(identifier[0]) if len(identifier) == 1 else "id"

    @classmethod
    def create_for_schema(cls, dataset: Dataset, table_schema: DatasetTableSchema) -> DatasetTable:
        """Create a DatasetTable object based on the Amsterdam Schema table spec.

        (The table spec contains a JSON-schema for all fields).
        """
        instance = cls(dataset=dataset)
        instance.save_for_schema(table_schema)
        return instance

    def save_for_schema(self, table_schema: DatasetTableSchema):
        """Save changes to the dataset table schema."""
        display_field = table_schema.display_field
        self.name = to_snake_case(table_schema.id)
        self.db_table = table_schema.db_name
        self.version = table_schema.version
        self.auth = " ".join(table_schema.auth)
        self.display_field = display_field.db_name if display_field is not None else None
        self.geometry_field, self.geometry_field_type = self._get_geometry_field(table_schema)
        self.is_temporal = table_schema.is_temporal
        self.enable_geosearch = (
            table_schema.dataset.id not in settings.AMSTERDAM_SCHEMA["geosearch_disabled_datasets"]
        )
        self.id_field = self._get_id_field(table_schema)

        is_creation = not self._state.adding
        self.save()

        new_definitions = {f.python_name: f for f in table_schema.fields}
        new_names = set(new_definitions.keys())
        existing_fields = {f.name: f for f in self.fields.all()} if is_creation else {}
        existing_names = set(existing_fields.keys())

        # Create new fields
        for added_name in new_names - existing_names:
            DatasetField.create_for_schema(self, field=new_definitions[added_name])

        # Update existing fields
        for changed_name in existing_names & new_names:
            field = new_definitions[changed_name]
            existing_fields[changed_name].save_for_schema(field=field)

        for removed_name in existing_names - new_names:
            existing_fields[removed_name].delete()


class DatasetField(models.Model):
    """Exposed metadata per field."""

    table = models.ForeignKey(DatasetTable, on_delete=models.CASCADE, related_name="fields")
    name = models.CharField(max_length=100)

    # Exposed metadata from the jsonschema, so other utils can query these
    auth = models.CharField(max_length=250, blank=True, null=True)

    class Meta:
        ordering = ("name",)
        verbose_name = _("Dataset Field")
        verbose_name_plural = _("Dataset Fields")
        unique_together = [
            ("table", "name"),
        ]

    def __str__(self):
        return self.name

    @classmethod
    def create_for_schema(cls, table: DatasetTable, field: DatasetFieldSchema) -> DatasetField:
        """Create a DatasetField object based on the Amsterdam Schema field spec."""
        instance = cls(table=table)
        instance.save_for_schema(field)
        return instance

    def save_for_schema(self, field: DatasetFieldSchema):
        """Update the field with the provided schema data."""
        self.name = to_snake_case(field.id)
        self.auth = " ".join(field.auth)
        self.save()


class Profile(models.Model):
    """User Profile."""

    name = models.CharField(max_length=100)
    scopes = models.CharField(max_length=255)
    schema_data = models.TextField(_("Amsterdam Schema Contents"), validators=[validate_json])

    def __str__(self):
        return self.name

    @cached_property
    def schema(self) -> ProfileSchema:
        """Provide access to the schema data"""
        if not self.schema_data:
            raise RuntimeError("Profile.schema_data is empty")

        return ProfileSchema.from_dict(json.loads(self.schema_data))

    def get_scopes(self):
        """The auth scopes for this profile"""
        return set(json.loads(self.scopes))

    @classmethod
    def create_for_schema(cls, profile_schema: ProfileSchema) -> Profile:
        """Create Profile object based on the Amsterdam Schema profile spec."""
        instance = cls()
        instance.save_for_schema(profile_schema)
        return instance

    def save_for_schema(self, profile_schema: ProfileSchema) -> Profile:
        self.name = profile_schema.name
        self.scopes = json.dumps(sorted(profile_schema.scopes))
        self.schema_data = profile_schema.json()
        self.save()
        return self


class LooseRelationField(models.ForeignKey):
    """A relation that points to one part of the composite key.

    This only points to the first field of the relation, and the second field
    (e.g. volgnummer/beginGeldigheid) is determined at runtime using query filtering.
    Without such filter, traversing the relation will produce multiple results.

    This setup is typically used for temporal relationships, where the foreign table uses compound
    keys as identifier. This field type references the first field of that compound relationship.

    At construction, the "to_field" will be set pointing to the correct identifier field.
    """

    # Disable the unique check made against the target field.
    # When it's part of a composite key, this can't always be unique by itself.
    # However, we can still have a db constraint on it.
    requires_unique_target = False

    def __init__(self, *args, **kwargs):
        if not kwargs.get("to_field"):
            raise ValueError("to_field must be provided.")
        super().__init__(*args, **kwargs)


class CompositeForeignKeyField(models.ForeignKey):
    """A composite key, based on multiple fields on the target table.

    Note this class is currently backed by a database column
    that holds a concatenated value in the database.
    So this acts as a tagging class only to reveal what the field type is.
    Ideally, this would have to change to an actual compound key field
    like Django's ``GenericForeignKey`` is. Also note that
    Django's ForeignObject has a ``to_fields`` attribute that supports
    referencing multiple fields.
    """

    def __init__(self, *args, to_fields, **kwargs):
        super().__init__(*args, **kwargs)

        # Currently, only used as descriptive data for analysis.
        # Called as _to_fields to avoid overriding the base class attribute.
        self._to_fields = to_fields

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["to_fields"] = self._to_fields
        return name, path, args, kwargs


class LooseRelationManyToManyField(models.ManyToManyField):
    pass
