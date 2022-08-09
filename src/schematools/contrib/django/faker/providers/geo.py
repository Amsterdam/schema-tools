"""Very rudimentary implementation of fake geo data for now."""
import logging
import random

from django.contrib.gis import geos
from factory import Faker
from faker.providers import BaseProvider
from gisserver.geometries import CRS, BoundingBox

LOCALE = "nl_NL"
RD_NEW_CRS_STR = "EPSG:28992"
RD_NEW_SRID = 28992
RD_NEW = CRS.from_string(RD_NEW_CRS_STR)
DEFAULT_CRS = RD_NEW

# In RD crs
WEST, SOUTH = 104000, 474000
EAST, NORTH = 136000, 501999

ADAM_BBOX = BoundingBox(south=SOUTH, west=WEST, north=NORTH, east=EAST, crs=RD_NEW)

logger = logging.getLogger(__name__)


class UnsupportedGEOTypeException(Exception):
    """Exception to signal that the generic Geometry type is used.

    This type is not usable, because a faker does not know what type of geo data is needed.
    """


def random_point(has_z=False):
    point = (random.uniform(WEST, EAST), random.uniform(SOUTH, NORTH))  # noqa: S311  # nosec: B311
    return point if not has_z else point + (0.0,)


class Geometry:
    def __init__(self, has_z=False):
        try:
            class_name = self.__class__.__name__
            shape_cls = getattr(geos, class_name)
            self.has_z = has_z
        except AttributeError:
            raise UnsupportedGEOTypeException(f"Class {class_name} is not allowed.")
        self.shape = shape_cls(*self.get_coordinates(has_z=self.has_z), srid=RD_NEW_SRID)

    def get_coordinates(self, has_z=False):
        return random_point(has_z=has_z)

    @property
    def ewkt(self):
        return self.shape.ewkt


class Point(Geometry):
    pass


class MultiPoint(Geometry):
    def get_coordinates(self, has_z=False):
        return [[random_point(has_z=has_z) for i in range(5)]]


class Polygon(Geometry):
    def get_coordinates(self, has_z=False):
        # Use a circle
        center = Point(has_z=has_z)
        return center.shape.buffer(1000)


class MultiPolygon(Geometry):
    def get_coordinates(self, has_z=False):
        return [[Polygon(has_z=has_z).shape for j in range(3)]]


class LineString(Geometry):
    def get_coordinates(self, has_z=False):
        return [[random_point(has_z=has_z) for i in range(5)]]


class MultiLineString(Geometry):
    def get_coordinates(self, has_z=False):
        return [[LineString(has_z=has_z).shape for j in range(3)]]


class GeometryCollection(Geometry):
    def get_coordinates(self, has_z=False):
        return [(Polygon(has_z=has_z).shape, LineString(has_z=has_z).shape)]


class GeoProvider(BaseProvider):  # noqa: D101
    """Geoprovider that selects a concrete implementation class."""

    def geo(self, geo_type: str = "Geometry", crs: CRS = DEFAULT_CRS) -> str:  # noqa: D102
        try:
            has_z = crs.srid == 7415  # maybe we need to refine this!
            shape = globals()[geo_type](has_z=has_z)
            if crs != DEFAULT_CRS:
                crs.apply_to(shape.shape)
            ewkt = shape.ewkt
        except (UnsupportedGEOTypeException, KeyError, TypeError) as e:
            message = str(e)
            logger.warning("Skipping geo data creation, reason: %s", message)
            return None
        else:
            return ewkt


Faker.add_provider(GeoProvider, locale=LOCALE)
