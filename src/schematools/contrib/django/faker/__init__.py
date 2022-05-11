from factory import Faker, Sequence  # noqa: D104
from gisserver.geometries import CRS

from schematools.contrib.django.faker.providers import (  # noqa: F401, this is to load provider
    intwindow,
)
from schematools.contrib.django.faker.providers.geo import RD_NEW_CRS_STR

LOCALE = "nl_NL"


# Mapping of jsonschema types and formats
# to faker-providers
# Additional paramaters for the provider can be defined.
FAKER_PROVIDER_LOOKUP = {
    "string": ("pystr", None),
    "integer": ("pyint", None),
    "integer/autoincrement": ("sequence", None),
    "string/autoincrement": ("sequence", None),
    "date": ("date", None),
    "date-time": ("date_time", None),
    "time": ("time", None),
    "number": ("pyfloat", None),
    "boolean": ("boolean", None),
    # "array": (ArrayField, None),  # XXX are we ever using this type?
    "object": ("pystr", None),  # needs a concatenated key field
    "/definitions/id": ("pyint", None),
    "/definitions/schema": ("text", None),
    "https://geojson.org/schema/Geometry.json": ("geo", {"geo_type": "Geometry"}),
    "https://geojson.org/schema/Point.json": ("geo", {"geo_type": "Point"}),
    "https://geojson.org/schema/MultiPoint.json": ("geo", {"geo_type": "MultiPoint"}),
    "https://geojson.org/schema/Polygon.json": ("geo", {"geo_type": "Polygon"}),
    "https://geojson.org/schema/MultiPolygon.json": ("geo", {"geo_type": "MultiPolygon"}),
    "https://geojson.org/schema/LineString.json": ("geo", {"geo_type": "LineString"}),
    "https://geojson.org/schema/MultiLineString.json": ("geo", {"geo_type": "MultiLineString"}),
    "https://geojson.org/schema/GeometryCollection.json": (
        "geo",
        {"geo_type": "GeometryCollection"},
    ),
    "city": ("city", None),
    "int_window": ("int_window", None),
    "bsn": ("ssn", None),
}


def get_field_factory(field_type: str, crs: str = RD_NEW_CRS_STR, elements=None) -> Faker:
    """Gets the appropriate field factory."""
    provider, kwargs = FAKER_PROVIDER_LOOKUP.get(field_type, ("text", None))

    # Sequence is a bit of a special case, because it is provided
    # by factory_boy and not by faker.
    if provider == "sequence":
        return Sequence(int)
    faker_kwargs = {} if kwargs is None else kwargs.copy()

    if elements is not None:
        provider = "random_element"
        faker_kwargs["elements"] = elements
    if provider == "geo":
        faker_kwargs["crs"] = CRS.from_string(crs)
    return Faker(provider, locale=LOCALE, **faker_kwargs)
