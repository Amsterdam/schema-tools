from __future__ import annotations  # noqa: D104

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, ClassVar
from urllib.parse import urlparse

from django.conf import settings
from factory import Faker as FactoryBoyFaker
from factory import LazyAttribute
from factory import Sequence as FactoryBoySequence
from factory.declarations import BaseDeclaration
from gisserver.geometries import CRS

from schematools.contrib.django.faker.providers import (  # noqa: F401, this is to load providers
    date,
    date_time,
    geo,
    integer,
    nuller,
    pyfloat,
)
from schematools.types import DatasetFieldSchema

LOCALE = "nl_NL"


@dataclass
class Declaration:
    """Sets up a BaseDeclaration and its parameters to be used for FactoryBoy.

    This wrapper class around BaseDeclaration enables a layered approach,
    where the parameters can be overridden at several levels.
    """

    cls: ClassVar[type] = BaseDeclaration
    arg: Any | None = None
    declaration_kwargs: dict[str, Any] = dataclass_field(default_factory=dict)
    field_kwargs: dict[str, Any] = dataclass_field(default_factory=dict)
    field_arg: Any | None = dataclass_field(init=False)

    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        """Get modifications to the args and kwarg for the __call__.

        When a Declaration is being used to create the actual mock data,
        for some mockers, additional or modified args or kwargs are needed.
        This method gives subclasses the opportunity to change arg or kwargs
        according to their needs.
        """
        return None, {}

    def __call__(self, field, **schema_faker_kwargs) -> BaseDeclaration:
        """Create the actual FactoryBoy declaration instance that is being used for mocking."""
        arg, kwargs = self.get_call_param_modifiers(field)
        call_kwargs = {}
        call_kwargs.update(self.declaration_kwargs)
        call_kwargs.update(kwargs)
        call_kwargs.update(schema_faker_kwargs)
        call_arg = arg or self.arg
        return self.cls(call_arg, **call_kwargs)


@dataclass
class Sequence(Declaration):
    """Declaration wrapper, wrapping the standard FactoryBoy Sequence."""

    cls: ClassVar = FactoryBoySequence
    arg: Callable[[], Any] = int


@dataclass
class Lazy(Declaration):
    """Declaration wrapper that is a base class for `LazyAttribute` based declarations."""

    cls: ClassVar = LazyAttribute


@dataclass
class PkJoiner(Lazy):
    """This declaration use the faker.LazyAttribute.

    So, the `self.arg` needs to be a function,
    that will be evaluated late in the mocking process.
    """

    def _join_field_values(self, obj):
        values = [str(getattr(obj, fid)) for fid in obj.table_schema.identifier]
        return ".".join(values)

    def __post_init__(self):
        self.arg = self._join_field_values


@dataclass
class Faker(Declaration):
    """Declarion wrapper base class for more specialized fakers."""

    cls: ClassVar = FactoryBoyFaker

    def __post_init__(self):
        self.declaration_kwargs["locale"] = LOCALE

    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        arg, kwargs = super().get_call_param_modifiers(field)
        if (elements := field.get("enum")) is not None:
            kwargs["elements"] = elements
            arg = "random_element"
        return arg, kwargs


@dataclass
class GeoFaker(Faker):
    """Declaration wrapper for the `geo` faker."""

    arg: str = "geo"  # The registered `geo` faker

    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        arg, kwargs = super().get_call_param_modifiers(field)
        return None, kwargs | {"crs": CRS.from_string(field.crs)}


@dataclass
class NullableFaker(Faker):
    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        arg, kwargs = super().get_call_param_modifiers(field)
        return None, kwargs | {"nullable": not field.required}


@dataclass
class NullableIntFaker(Faker):
    """Declaration wrapper for nullable integers.

    With support for `minimum` and `maximum`.
    """

    arg: str = "nullable_int"

    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        arg, kwargs = super().get_call_param_modifiers(field)
        if arg == "random_element":  # for enums, we return here
            return arg, kwargs
        if (min_ := field.get("minimum")) is not None:
            kwargs["min_"] = min_
        if (max_ := field.get("maximum")) is not None:
            kwargs["max_"] = max_
        return None, kwargs


@dataclass
class NullableFloatFaker(Faker):
    """Declaration wrapper for nullable floats.

    With support for `minimum` and `maximum`.
    """

    arg: str = "nullable_float"

    def get_call_param_modifiers(self, field) -> tuple(Any, dict[str, Any]):
        arg, kwargs = super().get_call_param_modifiers(field)
        if (min_value := field.get("minimum")) is not None:
            kwargs["min_value"] = min_value
        if (max_value := field.get("maximum")) is not None:
            kwargs["max_value"] = max_value
        return None, kwargs


DECLARATION_LOOKUP = {
    "string": Faker("pystr"),
    "integer": NullableIntFaker(),
    "integer/autoincrement": Sequence(),
    "string/autoincrement": Sequence(lambda n: str(n)),
    "number/autoincrement": Sequence(),
    "date": NullableFaker("nullable_date_object"),
    "date-time": NullableFaker("nullable_date_time"),
    "time": Faker("time"),
    "number": NullableFloatFaker(),
    "boolean": Faker("boolean"),
    "array": Faker("pylist", {"value_types": [str]}),
    "object": Faker("pystr"),  # needs a concatenated key field
    "json": Faker("json"),
    "/definitions/id": Faker("pyint"),
    "/definitions/schema": Faker("text"),
    "https://geojson.org/schema/Geometry.json": GeoFaker(
        declaration_kwargs={"geo_type": "Geometry"}
    ),
    "https://geojson.org/schema/Point.json": GeoFaker(declaration_kwargs={"geo_type": "Point"}),
    "https://geojson.org/schema/MultiPoint.json": GeoFaker(
        declaration_kwargs={"geo_type": "MultiPoint"}
    ),
    "https://geojson.org/schema/Polygon.json": GeoFaker(
        declaration_kwargs={"geo_type": "Polygon"}
    ),
    "https://geojson.org/schema/MultiPolygon.json": GeoFaker(
        declaration_kwargs={"geo_type": "MultiPolygon"}
    ),
    "https://geojson.org/schema/LineString.json": GeoFaker(
        declaration_kwargs={"geo_type": "LineString"}
    ),
    "https://geojson.org/schema/MultiLineString.json": GeoFaker(
        declaration_kwargs={"geo_type": "MultiLineString"}
    ),
    "https://geojson.org/schema/GeometryCollection.json": GeoFaker(
        declaration_kwargs={"geo_type": "GeometryCollection"},
    ),
    "city": Faker("city"),
    "sequence": Faker("sequence"),
    "bsn": Faker("ssn"),
    "street_name": Faker("street_name"),
    "nuller": Faker("nuller"),
    "joiner": PkJoiner(),
    "uri": Faker("uri"),
    "email": Faker("email"),
}


def get_field_factory(field: DatasetFieldSchema) -> BaseDeclaration:
    """Gets the appropriate field factory."""
    field_type = field.type
    # reduce amsterdam schema refs to their fragment
    if field_type.startswith(settings.SCHEMA_DEFS_URL):
        field_type = urlparse(field_type).fragment

    # If, in addition to a type, a format has been defined
    # for a field, the format will be used to look up the
    # appropriate provider.
    if (format_ := field.format) is not None:
        field_type = format_

    # If a faker has been defined for the field, this
    # faker is used for the provider lookup.
    schema_faker_kwargs = {}
    if (schema_faker := field.faker) is not None:
        field_type = schema_faker.name
        schema_faker_kwargs = schema_faker.properties
    declaration_maker = DECLARATION_LOOKUP.get(field_type)
    if declaration_maker is None:
        raise ValueError(f"No declaration defined for field with type {field_type}")

    return declaration_maker(field, **schema_faker_kwargs)
