"""The Django models for Amsterdam Schema data.

When models are generated with :func:`~schematools.contrib.django.factories.model_factory`,
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
from typing import Any, Dict, List, Optional, Tuple, Type

from django.apps import apps
from django.conf import settings
from django.contrib.gis.db import models as gis_models
from django.contrib.postgres.fields import ArrayField
from django.db import models, transaction
from django.db.models import JSONField
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_postgres_unlimited_varchar import UnlimitedCharField
from gisserver.types import CRS

from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    ProfileSchema,
    SchemaType,
)
from schematools.utils import to_snake_case

from . import managers
from .validators import URLPathValidator

logger = logging.getLogger(__name__)

GEOJSON_PREFIX = "https://geojson.org/schema/"

FORMAT_MODELS_LOOKUP = {
    "date": models.DateField,
    "time": models.TimeField,
    "date-time": models.DateTimeField,
    "uri": models.URLField,
    "email": models.EmailField,
    "blob-azure": UnlimitedCharField,
}

RD_NEW = CRS.from_string("EPSG:28992")  # Amersfoort / RD New

TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]


class ObjectMarker:
    """Class to signal that field type object has been found in the aschema definition.
    For FK and NM relations, this class will be replaced by another field class,
    during processing (in the FieldMaker).
    For non-model fields (e.g. BRP), this class marks the fact that no
    model field needs to be generated.
    """

    pass


def _fetch_srid(dataset: DatasetSchema, field: DatasetFieldSchema) -> Dict[str, Any]:
    return {"srid": CRS.from_string(dataset.data["crs"]).srid}


JSON_TYPE_TO_DJANGO = {
    "string": (UnlimitedCharField, None),
    "integer": (models.BigIntegerField, None),
    "integer/autoincrement": (models.AutoField, None),
    "date": (models.DateField, None),
    "datetime": (models.DateTimeField, None),
    "time": (models.TimeField, None),
    "number": (models.FloatField, None),
    "boolean": (models.BooleanField, None),
    "array": (ArrayField, None),
    "object": (ObjectMarker, None),
    "/definitions/id": (models.IntegerField, None),
    "/definitions/schema": (UnlimitedCharField, None),
    "https://geojson.org/schema/Geometry.json": (
        gis_models.GeometryField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/Point.json": (
        gis_models.PointField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/MultiPoint.json": (
        gis_models.MultiPointField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/Polygon.json": (
        gis_models.PolygonField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/MultiPolygon.json": (
        gis_models.MultiPolygonField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/LineString.json": (
        gis_models.LineStringField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/MultiLineString.json": (
        gis_models.MultiLineStringField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
    "https://geojson.org/schema/GeometryCollection.json": (
        gis_models.GeometryCollectionField,
        {"value_getter": _fetch_srid, "srid": RD_NEW.srid, "geography": False, "db_index": True},
    ),
}


class DynamicModel(models.Model):
    """Base class to tag and detect dynamically generated models."""

    #: Overwritten by subclasses / factory
    CREATION_COUNTER = None
    _dataset: Dataset = None  # type: ignore[assignment]
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
        return cls._dataset.schema.id

    @classmethod
    def get_dataset_path(cls) -> str:
        """Give access to the api path this dataset should be published on."""
        return cls._dataset.path

    @classmethod
    def get_dataset_schema(cls) -> DatasetSchema:
        """Give access to the original dataset schema that this model is a part of."""
        return cls._dataset.schema

    @classmethod
    def table_schema(cls) -> DatasetTableSchema:
        """Give access to the original table_schema that this model implements."""
        return cls._table_schema

    @classmethod
    def get_table_id(cls) -> str:
        """Give access to the table name"""
        return cls._table_schema.id

    @classmethod
    def has_parent_table(cls) -> bool:
        """Check if table is sub table for another table."""
        return cls._table_schema.has_parent_table or cls._table_schema.is_through_table

    @classmethod
    def has_display_field(cls) -> bool:
        """Tell whether a display field is configured."""
        return cls._display_field is not None

    @classmethod
    def get_display_field(cls) -> Optional[str]:
        """Return the name of the display field, for usage by Django models."""
        return cls._display_field

    @classmethod
    def is_temporal(cls) -> bool:
        """Indicates if this model has temporary characteristics."""
        return cls._is_temporal


class Dataset(models.Model):
    """A registry of all available datasets that are uploaded in the API server.

    Each model holds the contents of an "Amsterdam Schema",
    that contains multiple tables.
    """

    name = models.CharField(_("Name"), unique=True, max_length=50)
    schema_data = JSONField(_("Amsterdam Schema Contents"))
    version = models.CharField(_("Schema Version"), blank=True, null=True, max_length=250)
    is_default_version = models.BooleanField(_("Default version"), default=False)

    # Settings for publishing the schema:
    enable_api = models.BooleanField(default=True)
    enable_db = models.BooleanField(default=True)
    endpoint_url = models.URLField(blank=True, null=True)
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

        # The check makes sure that deferred fields are not checked for changes,
        # nor that creating the model
        self._old_schema_data = (
            self.schema_data if "schema_data" in self.__dict__ and not self._state.adding else None
        )

    @classmethod
    def name_from_schema(cls, schema: DatasetSchema) -> str:
        """Generate dataset name from schema"""
        name = to_snake_case(schema.id)
        if schema.version and not schema.is_default_version:
            name = to_snake_case(f"{name}_{schema.version}")
        return name

    @classmethod
    def create_for_schema(cls, schema: DatasetSchema, path: Optional[str] = None) -> Dataset:
        """Create the schema based on the Amsterdam Schema JSON input"""
        name = cls.name_from_schema(schema)
        if path is None:
            path = schema.get("path", name)
        return cls.objects.create(
            name=name,
            schema_data=schema.json_data(),
            auth=" ".join(schema.auth),
            path=path,
            version=schema.version,
            is_default_version=schema.is_default_version,
        )

    def save_for_schema(self, schema: DatasetSchema):
        """Update this model with schema data"""
        self.schema_data = schema.json_data()
        self.auth = " ".join(schema.auth)

        if self.schema_data_changed():
            self.save(update_fields=["schema_data", "auth", "is_default_version", "path"])
            return True
        else:
            return False

    def save(self, *args, **kwargs):
        """Perform a final data validation check, and additional updates."""
        if "schema_data" in self.__dict__:
            # Make sure the schema_data field is properly filled with an actual dict.
            if self.schema_data and not isinstance(self.schema_data, dict):
                logger.debug(
                    "Invalid data in Dataset.schema_data, expected dict: %r",
                    self.schema_data,
                )
                raise RuntimeError("Invalid data in Dataset.schema_data")

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

    def save_schema_tables(self):
        """Expose the schema data to the DatasetTable.
        This allows other projects (e.g. geosearch) to process our dynamic tables.
        """
        if not self.schema_data:
            # no schema stored -> no tables
            if self._old_schema_data:
                self.tables.all().delete()
            return

        new_definitions = {
            to_snake_case(t.id): t for t in self.schema.get_tables(include_nested=True)
        }
        new_names = set(new_definitions.keys())
        existing_models = {t.name: t for t in self.tables.all()}
        existing_names = set(existing_models.keys())

        # Create models for newly added tables
        for added_name in new_names - existing_names:
            table = new_definitions[added_name]
            DatasetTable.create_for_schema(self, table)

        # Update models for updated tables
        for changed_name in existing_names & new_names:
            table = new_definitions[changed_name]
            existing_models[changed_name].save_for_schema(table)

        # Remove tables that are no longer part of the schema.
        for removed_name in existing_names - new_names:
            existing_models[removed_name].delete()

    save_schema_tables.alters_data = True

    @cached_property
    def schema(self) -> DatasetSchema:
        """Provide access to the schema data"""
        if not self.schema_data:
            raise RuntimeError("Dataset.schema_data is empty")

        return DatasetSchema.from_dict(self.schema_data)

    @cached_property
    def has_geometry_fields(self) -> bool:
        return any(field.is_geo for table in self.schema.tables for field in table.fields)

    def schema_data_changed(self):
        """Check whether the schema_data attribute changed"""
        return (
            "schema_data" in self.__dict__  # this checks for deferred attributes
            and self.schema_data != self._old_schema_data
        )

    def create_models(self, base_app_name: Optional[str] = None) -> List[Type[DynamicModel]]:
        """Extract the models found in the schema"""
        from schematools.contrib.django.factories import schema_models_factory

        if not self.enable_db:
            return []
        else:
            return schema_models_factory(self, base_app_name=base_app_name)


class DatasetTable(models.Model):
    """Exposed metadata per schema.

    This table can be read by the 'geosearch' project to locate all our tables and data sources.
    """

    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="tables")
    name = models.CharField(max_length=100)

    # Exposed metadata from the jsonschema, so other utils can query these
    auth = models.CharField(max_length=250, blank=True, null=True)
    enable_geosearch = models.BooleanField(default=True)
    db_table = models.CharField(max_length=100)
    display_field = models.CharField(max_length=50, null=True, blank=True)
    geometry_field = models.CharField(max_length=50, null=True, blank=True)
    geometry_field_type = models.CharField(max_length=50, null=True, blank=True)
    is_temporal = models.BooleanField(null=False, blank=False, default=False)

    class Meta:
        ordering = ("name",)
        verbose_name = _("Dataset Table")
        verbose_name_plural = _("Dataset Tables")
        unique_together = [
            ("dataset", "name"),
        ]

    def __str__(self):
        return self.name

    @classmethod
    def _get_field_values(cls, table_schema):
        ret = {
            "display_field": table_schema.display_field,
            "geometry_field": None,
            "geometry_field_type": None,
            "is_temporal": table_schema.is_temporal,
        }

        for field in table_schema.fields:
            # Take the first geojson field as geometry field
            if not ret["geometry_field"] and field.type.startswith(GEOJSON_PREFIX):
                ret["geometry_field"] = field.name
                match = re.search(r"schema\/(?P<schema>\w+)\.json", field.type)
                if match is not None:
                    ret["geometry_field_type"] = match.group("schema")
                break
        return ret

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
        self.name = to_snake_case(table_schema.id)
        self.db_table = table_schema.db_name()
        self.auth = " ".join(table_schema.auth)
        self.display_field = (
            to_snake_case(table_schema.display_field) if table_schema.display_field else None
        )
        self.enable_geosearch = (
            table_schema.dataset.id not in settings.AMSTERDAM_SCHEMA["geosearch_disabled_datasets"]
        )
        for field, value in self._get_field_values(table_schema).items():
            setattr(self, field, value)

        is_creation = not self._state.adding
        self.save()

        new_definitions = {to_snake_case(f.id): f for f in table_schema.fields}
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
    schema_data = JSONField(_("Amsterdam Schema Contents"))

    @cached_property
    def schema(self) -> ProfileSchema:
        """Provide access to the schema data"""
        if not self.schema_data:
            raise RuntimeError("Profile.schema_data is empty")

        return ProfileSchema.from_dict(self.schema_data)

    def get_scopes(self):
        """The auth scopes for this profile"""
        return json.loads(self.scopes.replace("'", '"'))

    @classmethod
    def create_for_schema(cls, profile_schema: ProfileSchema) -> Profile:
        """Create Profile object based on the Amsterdam Schema profile spec."""
        instance = cls()
        instance.save_for_schema(profile_schema)
        return instance

    def save_for_schema(self, profile_schema: ProfileSchema) -> Profile:
        self.name = profile_schema.name
        self.scopes = profile_schema.scopes
        self.schema_data = profile_schema.json_data()
        self.save()
        return self

    def __str__(self):
        return self.name


class LooseRelationField(models.CharField):
    def __init__(self, *args, **kwargs):
        self.relation = kwargs.pop("relation")
        kwargs.setdefault("max_length", 254)
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        del kwargs["max_length"]
        kwargs["relation"] = self.relation
        return name, path, args, kwargs

    @property
    def related_model(self):
        """Add ``related_model`` like all other Django relational fields do."""
        dataset_name, table_name, *_ = [to_snake_case(part) for part in self.relation.split(":")]
        return apps.all_models[dataset_name][table_name]


class LooseRelationManyToManyField(models.ManyToManyField):
    def __init__(self, *args, **kwargs):
        self.relation = kwargs.pop("relation")
        kwargs.setdefault("max_length", 254)
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        del kwargs["max_length"]
        kwargs["relation"] = self.relation
        return name, path, args, kwargs

    @property
    def related_model(self):
        dataset_name, table_name, *_ = [to_snake_case(part) for part in self.relation.split(":")]
        if dataset_name in apps.all_models and table_name in apps.all_models[dataset_name]:
            return apps.all_models[dataset_name][table_name]
        else:
            #  The loosely related model may not be registered yet
            return None


def get_field_schema(model_field: models.Field) -> SchemaType:
    """Provide access to the underlying amsterdam schema field that created the model field."""
    if isinstance(model_field, models.ForeignObjectRel):
        # created by 'related_name' setting
        return model_field.remote_field.field_schema
    else:
        return model_field.field_schema
