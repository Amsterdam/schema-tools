from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Type

from django.apps import apps
from django.conf import settings
from django.core.cache import cache
from django.contrib.gis.db import models as gis_models
from django.contrib.postgres.fields import ArrayField, JSONField
from django.db import models, transaction
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_postgres_unlimited_varchar import UnlimitedCharField
from gisserver.types import CRS
from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    ProfileSchema,
    get_db_table_name,
)
from schematools.utils import to_snake_case


from . import managers
from .validators import URLPathValidator

logger = logging.getLogger(__name__)

GEOJSON_PREFIX = "https://geojson.org/schema/"
PROFILES_CACHE_KEY = "SCHEMATOOLS_AUTH_PROFILES"

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


def fetch_srid(dataset: DatasetSchema, field: DatasetFieldSchema) -> Dict[str, Any]:
    return {"srid": CRS.from_string(dataset.data["crs"]).srid}


JSON_TYPE_TO_DJANGO = {
    "string": (UnlimitedCharField, None),
    "integer": (models.IntegerField, None),
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
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/Point.json": (
        gis_models.PointField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiPoint.json": (
        gis_models.MultiPointField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/Polygon.json": (
        gis_models.PolygonField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiPolygon.json": (
        gis_models.MultiPolygonField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/LineString.json": (
        gis_models.LineStringField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiLineString.json": (
        gis_models.MultiLineStringField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
    "https://geojson.org/schema/GeometryCollection.json": (
        gis_models.GeometryCollectionField,
        dict(
            value_getter=fetch_srid,
            srid=RD_NEW.srid,
            geography=False,
            db_index=True,
        ),
    ),
}


class DynamicModel(models.Model):
    """Base class to tag and detect dynamically generated models."""

    #: Overwritten by subclasses / factory
    _table_schema: DatasetTableSchema = None
    _display_field = None

    class Meta:
        abstract = True

    def __str__(self):
        if self._display_field:
            return str(getattr(self, self._display_field))
        else:
            # this will not be shown in the dso-api view
            # and will be omitted when display field is empty or not present
            return f"(no title: {self._meta.object_name} #{self.pk})"

    # These classmethods could have been a 'classproperty',
    # but this ensures the names don't conflict with fields from the schema.

    @classmethod
    def get_dataset(cls) -> DatasetSchema:
        """Give access to the original dataset that this model is a part of."""
        return cls._table_schema._parent_schema

    @classmethod
    def table_schema(cls) -> DatasetTableSchema:
        """Give access to the original table_schema that this model implements."""
        return cls._table_schema

    @classmethod
    def get_dataset_id(cls) -> str:
        return cls._table_schema.dataset.id

    @classmethod
    def get_table_id(cls) -> str:
        """Give access to the table name"""
        return cls._table_schema.id

    @classmethod
    def has_parent_table(cls):
        """
        Check if table is sub table for another table.
        """
        return cls._table_schema.get("schema", {}).get("parentTableID") is not None

    @classmethod
    def has_display_field(cls):
        """Tell whether a display field is configured."""
        return cls._display_field is not None

    @classmethod
    def get_display_field(cls):
        """Return the name of the display field, for usage by Django models."""
        return cls._display_field

    @classmethod
    def is_temporal(cls):
        """Indicates if this model has temporary characteristics."""
        return cls._is_temporal


class Dataset(models.Model):
    """A registry of all available datasets that are uploaded in the API server.

    Each model holds the contents of an "Amsterdam Schema",
    that contains multiple tables.
    """

    name = models.CharField(_("Name"), unique=True, max_length=50)
    schema_data = JSONField(_("Amsterdam Schema Contents"))

    # Settings for publishing the schema:
    enable_api = models.BooleanField(default=True)
    enable_db = models.BooleanField(default=True)
    endpoint_url = models.URLField(blank=True, null=True)
    url_prefix = models.CharField(
        max_length=100, blank=True, validators=[URLPathValidator()]
    )
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
            self.schema_data
            if "schema_data" in self.__dict__ and not self._state.adding
            else None
        )

    @classmethod
    def create_for_schema(cls, schema: DatasetSchema) -> Dataset:
        """Create the schema based on the Amsterdam Schema JSON input"""
        return cls.objects.create(
            name=to_snake_case(schema.id),
            schema_data=schema.json_data(),
            auth=_serialize_claims(schema),
        )

    def save_for_schema(self, schema: DatasetSchema):
        """Update this model with schema data"""
        self.schema_data = schema.json_data()
        self.auth = _serialize_claims(schema)

        if self.schema_data_changed():
            self.save(update_fields=["schema_data", "auth"])
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

    def schema_data_changed(self):
        """Check whether the schema_data attribute changed"""
        return (
            "schema_data" in self.__dict__  # this checks for deferred attributes
            and self.schema_data != self._old_schema_data
        )

    def create_models(self) -> List[Type[DynamicModel]]:
        """Extract the models found in the schema"""
        from schematools.contrib.django.factories import schema_models_factory

        if not self.enable_db:
            return []
        else:
            return schema_models_factory(self.schema)


class DatasetTable(models.Model):
    """Exposed metadata per schema.

    This table can be read by the 'geosearch' project to locate all our tables and data sources.
    """

    dataset = models.ForeignKey(
        Dataset, on_delete=models.CASCADE, related_name="tables"
    )
    name = models.CharField(max_length=100)

    # Exposed metadata from the jsonschema, so other utils can query these
    auth = models.CharField(max_length=250, blank=True, null=True)
    enable_geosearch = models.BooleanField(default=True)
    db_table = models.CharField(max_length=100, unique=True)
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
    def _get_field_values(cls, table):
        ret = {
            "display_field": table.display_field,
            "geometry_field": None,
            "geometry_field_type": None,
            "is_temporal": table.is_temporal,
        }

        for field in table.fields:
            # Take the first geojson field as geometry field
            if not ret["geometry_field"] and field.type.startswith(GEOJSON_PREFIX):
                ret["geometry_field"] = field.name
                match = re.search(r"schema\/(?P<schema>\w+)\.json", field.type)
                if match is not None:
                    ret["geometry_field_type"] = match.group("schema")
                break
        return ret

    @classmethod
    def create_for_schema(
        cls, dataset: Dataset, table: DatasetTableSchema
    ) -> DatasetTable:
        """Create a DatasetTable object based on the Amsterdam Schema table spec.

        (The table spec contains a JSON-schema for all fields).
        """
        instance = cls(dataset=dataset)
        instance.save_for_schema(table)
        return instance

    def save_for_schema(self, table: DatasetTableSchema):
        """Save changes to the dataset table."""
        self.name = to_snake_case(table.id)
        self.db_table = get_db_table_name(table)
        self.auth = _serialize_claims(table)
        self.display_field = (
            to_snake_case(table.display_field) if table.display_field else None
        )
        self.enable_geosearch = (
            table.dataset.id
            not in settings.AMSTERDAM_SCHEMA["geosearch_disabled_datasets"]
        )
        for field, value in self._get_field_values(table).items():
            setattr(self, field, value)

        is_creation = not self._state.adding
        self.save()

        new_definitions = {to_snake_case(f.name): f for f in table.fields}
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

    table = models.ForeignKey(
        DatasetTable, on_delete=models.CASCADE, related_name="fields"
    )
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
    def create_for_schema(
        cls, table: DatasetTable, field: DatasetFieldSchema
    ) -> DatasetField:
        """Create a DatasetField object based on the Amsterdam Schema field spec."""
        instance = cls(table=table)
        instance.save_for_schema(field)
        return instance

    def save_for_schema(self, field: DatasetFieldSchema):
        """Update the field with the provided schema data."""
        self.name = to_snake_case(field.name)
        self.auth = _serialize_claims(field)
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

    def get_permissions(self) -> Dict[str, str]:
        """
        Get Flattened permissions per profile in form:
        {dataset} = "permission"
        {dataset}:{table} = "permission"
        {dataset}:{table}:{field} = "permission"
        """
        permissions = dict()
        for dataset_id, dataset_settings in self.schema.datasets.items():
            dataset_key = generate_permission_key(dataset_id)
            if dataset_settings.permissions:
                permissions[dataset_key] = dataset_settings.permissions

            for table_id, table_settings in dataset_settings.tables.items():
                dataset_table_key = generate_permission_key(dataset_id, table_id)
                if table_settings.permissions:
                    permissions[dataset_table_key] = table_settings.permissions

                for field_id, field_permission in table_settings.fields.items():
                    permissions[
                        generate_permission_key(dataset_id, table_id, field_id)
                    ] = field_permission

        return permissions

    def get_scopes(self):
        try:
            return json.loads(self.scopes.replace("'", '"'))
        except ValueError:
            return []

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


def _serialize_claims(schema_object) -> Optional[str]:
    """Convert the schema/table/field auth claims to a string format"""
    claims = schema_object.get("auth")
    if not claims:
        return None
    elif isinstance(claims, str):
        return claims
    else:
        return " ".join(claims)


def generate_permission_key(*args):
    return ":".join([to_snake_case(key) for key in args])


def split_permission_key(key):
    return key.split(":")


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
        dataset_name, table_name, column_name = [
            to_snake_case(part) for part in self.relation.split(":")
        ]
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
        dataset_name, table_name, column_name = [
            to_snake_case(part) for part in self.relation.split(":")
        ]
        if (
            dataset_name in apps.all_models
            and table_name in apps.all_models[dataset_name]
        ):
            return apps.all_models[dataset_name][table_name]
        else:
            #  The loosely related model may not be registered yet
            return None


def get_active_profiles():
    profiles = cache.get(PROFILES_CACHE_KEY)
    if profiles is None:
        try:
            profiles = [profile for profile in Profile.objects.all()]
        except Exception:
            # Return empty list and do not cache result.
            profiles = []
        else:
            cache.set(PROFILES_CACHE_KEY, profiles)
    return profiles


get_active_profiles()
