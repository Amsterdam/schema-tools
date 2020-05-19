from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Type

from django.conf import settings
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
    get_db_table_name,
    is_possible_display_field,
)
from string_utils import slugify

from . import managers
from .validators import URLPathValidator

logger = logging.getLogger(__name__)

GEOJSON_PREFIX = "https://geojson.org/schema/"

DATE_MODELS_LOOKUP = {
    "date": models.DateField,
    "time": models.TimeField,
    "date-time": models.DateTimeField,
}

RD_NEW = CRS.from_string("EPSG:28992")  # Amersfoort / RD New

TypeAndSignature = Tuple[Type[models.Field], tuple, Dict[str, Any]]


def fetch_srid(dataset: DatasetSchema, field: DatasetFieldSchema) -> Dict[str, Any]:
    return {"srid": CRS.from_string(dataset.data["crs"]).srid}


JSON_TYPE_TO_DJANGO = {
    "string": (UnlimitedCharField, None),
    "integer": (models.IntegerField, None),
    "integer/autoincrement": (models.AutoField, None),
    "date": (models.DateField, None),
    "time": (models.TimeField, None),
    "number": (models.FloatField, None),
    "boolean": (models.BooleanField, None),
    "array": (ArrayField, None),
    "/definitions/id": (models.IntegerField, None),
    "/definitions/schema": (UnlimitedCharField, None),
    "https://geojson.org/schema/Geometry.json": (
        gis_models.GeometryField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/Point.json": (
        gis_models.PointField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiPoint.json": (
        gis_models.MultiPointField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/Polygon.json": (
        gis_models.PolygonField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiPolygon.json": (
        gis_models.MultiPolygonField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/LineString.json": (
        gis_models.LineStringField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/MultiLineString.json": (
        gis_models.MultiLineStringField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
        ),
    ),
    "https://geojson.org/schema/GeometryCollection.json": (
        gis_models.GeometryCollectionField,
        dict(
            value_getter=fetch_srid, srid=RD_NEW.srid, geography=False, db_index=True,
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
            return getattr(self, self._display_field)
        else:
            return f"(no title: {self._meta.object_name} #{self.pk})"

    # These classmethods could have been a 'classproperty',
    # but this ensures the names don't conflict with fields from the schema.

    @classmethod
    def get_dataset(cls) -> DatasetSchema:
        """Give access to the original dataset that this model is a part of."""
        return cls._table_schema._parent_schema

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
            name=slugify(schema.id, "_"),
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
            slugify(t.id, "_"): t for t in self.schema.get_tables(include_nested=True)
        }
        new_names = set(new_definitions.keys())
        existing_models = {t.name: t for t in self.tables.all()}
        existing_names = set(existing_models.keys())

        # Create models for newly added tables
        for added_name in new_names - existing_names:
            table = new_definitions[added_name]
            DatasetTable.create_for_schema(self, table)

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
        ret = {}

        # XXX For now, be OK with missing "display", is mandatory in aschema v1.1.1
        ret["display_field"] = table["schema"].get("display")
        ret["geometry_field"] = None
        ret["geometry_field_type"] = None
        for field in table.fields:
            # Take the first geojson field as geometry field
            if not ret["geometry_field"] and field.type.startswith(GEOJSON_PREFIX):
                ret["geometry_field"] = field.name
                match = re.search(r"schema\/(?P<schema>\w+)\.json", field.type)
                if match is not None:
                    ret["geometry_field_type"] = match.group("schema")
                break

            # Take the first string field as display name.
            if not ret["display_field"] and is_possible_display_field(field):
                ret["display_field"] = field.name

            if ret["display_field"] and ret["geometry_field"]:
                break
        return ret

    @classmethod
    def create_for_schema(
        cls, dataset: Dataset, table: DatasetTableSchema
    ) -> DatasetTable:
        """Create a DatasetTable object based on the Amsterdam Schema table spec.

        (The table spec contains a JSON-schema for all fields).
        """
        enable_geosearch = True
        if dataset.name in settings.AMSTERDAM_SCHEMA["geosearch_disabled_datasets"]:
            enable_geosearch = False

        instance = cls.objects.create(
            dataset=dataset,
            name=slugify(table.id, "_"),
            db_table=get_db_table_name(table),
            auth=_serialize_claims(table),
            enable_geosearch=enable_geosearch,
            **cls._get_field_values(table),
        )

        for field in table.fields:
            DatasetField.create_for_schema(instance, field)

        return instance


class DatasetField(models.Model):
    """Exposed metadata per field.
    """

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
        cls, table: DatasetTableSchema, field: DatasetFieldSchema
    ) -> DatasetField:
        """Create a DatasetField object based on the Amsterdam Schema field spec.

        """
        return cls.objects.create(
            table=table, name=slugify(field.name, "_"), auth=_serialize_claims(field)
        )


def _serialize_claims(schema_object) -> Optional[str]:
    """Convert the schema/table/field auth claims to a string format"""
    claims = schema_object.get("auth")
    if not claims:
        return None
    elif isinstance(claims, str):
        return claims
    else:
        return " ".join(claims)
