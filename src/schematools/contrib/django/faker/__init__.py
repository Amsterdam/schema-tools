from __future__ import annotations  # noqa: D104

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Callable, ClassVar
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
    """Sets up a BaseDeclaration and it parameters to be used for FactoryBoy.

    This wrapper class around BaseDeclaration enables a layered approach,
    where the parameters can be overridden a several levels.
    """

    cls: ClassVar[type] = BaseDeclaration
    arg: Any | None = None
    declaration_kwargs: dict[str, Any] = dataclass_field(default_factory=dict)
    field_kwargs: dict[str, Any] = dataclass_field(default_factory=dict)
    field_arg: Any | None = dataclass_field(init=False)

    def set_params(self, field) -> None:  # noqa: D102
        self.field_arg = None
        self.field_kwargs = {}

    def __call__(self, **schema_faker_kwargs) -> BaseDeclaration:
        call_kwargs = {}
        call_kwargs.update(self.declaration_kwargs)
        call_kwargs.update(self.field_kwargs)
        call_kwargs.update(schema_faker_kwargs)
        call_arg = self.field_arg or self.arg
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

    def set_params(self, field: DatasetFieldSchema):  # noqa: D102
        super().set_params(field)
        if (elements := field.get("enum")) is not None:
            self.field_kwargs["elements"] = elements
            self.field_arg = "random_element"


@dataclass
class GeoFaker(Faker):
    """Declaration wrapper for the `geo` faker."""

    arg: str = "geo"  # The registered `geo` faker

    def set_params(self, field: DatasetFieldSchema):  # noqa: D102
        super().set_params(field)
        self.field_kwargs["crs"] = CRS.from_string(field.crs)


@dataclass
class NullableFaker(Faker):
    def set_params(self, field: DatasetFieldSchema):  # noqa: D102
        super().set_params(field)
        self.field_kwargs["nullable"] = not field.required


@dataclass
class NullableIntFaker(Faker):
    """Declaration wrapper for nullable integers.

    With support for `minimum` and `maximum`.
    """

    arg: str = "nullable_int"

    def set_params(self, field: DatasetFieldSchema):  # noqa: D102
        super().set_params(field)
        if (min_ := field.get("minimum")) is not None:
            self.field_kwargs["min_"] = min_
        if (max_ := field.get("maximum")) is not None:
            self.field_kwargs["max_"] = max_


@dataclass
class NullableFloatFaker(Faker):
    """Declaration wrapper for nullable floats.

    With support for `minimum` and `maximum`.
    """

    arg: str = "nullable_float"

    def set_params(self, field: DatasetFieldSchema):  # noqa: D102
        super().set_params(field)
        if (min_value := field.get("minimum")) is not None:
            self.field_kwargs["min_value"] = min_value
        if (max_value := field.get("maximum")) is not None:
            self.field_kwargs["max_value"] = max_value


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

    declaration_maker.set_params(field)
    return declaration_maker(**schema_faker_kwargs)
