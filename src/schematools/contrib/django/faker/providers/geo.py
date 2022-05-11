"""Very rudimentary implementation of fake geo data for now.
"""
import logging

from factory import Faker
from faker.providers import BaseProvider
from gisserver.geometries import CRS
from shapely import geometry

LOCALE = "nl_NL"
RD_NEW_CRS_STR = "EPSG:28992"
DEFAULT_CRS = CRS.from_string(RD_NEW_CRS_STR)


ADAM_BBOX_LL_SOUTH_WEST = (52.25168, 4.64034)
ADAM_BBOX_LL_NORTH_EAST = (52.50536, 5.10737)

ADAM_BBOX = geometry.box(*(ADAM_BBOX_LL_SOUTH_WEST + ADAM_BBOX_LL_NORTH_EAST))


logger = logging.getLogger(__name__)


class UnsupportedGEOTypeException(Exception):
    """Exception to signal that the generic Geometry type is used.

    This type is not usable, because a faker does not know what type of geo data is needed.
    """


def random_point():
    point_in_box = ADAM_BBOX.representative_point()
    return point_in_box.coords[0]


class Geometry:
    def __init__(self):
        try:
            class_name = self.__class__.__name__
            shape_cls = getattr(geometry, class_name)
        except AttributeError:
            raise UnsupportedGEOTypeException(f"Class {class_name} is not allowed.")
        self.shape = shape_cls(*self.get_coordinates())

    def get_coordinates(self):
        return random_point()

    @property
    def wkt(self):
        return self.shape.wkt


class Point(Geometry):
    pass


class MultiPoint(Geometry):
    def get_coordinates(self):
        return [[random_point() for i in range(5)]]


class Polygon(Geometry):
    def get_coordinates(self):
        return [[random_point() for i in range(5)]]


class MultiPolygon(Geometry):
    def get_coordinates(self):
        return [[Polygon().shape for j in range(3)]]


class LineString(Geometry):
    def get_coordinates(self):
        return [[random_point() for i in range(5)]]


class MultiLineString(Geometry):
    def get_coordinates(self):
        return [[LineString().shape for j in range(3)]]


class GeometryCollection(Geometry):
    def get_coordinates(self):
        return [(Polygon().shape, LineString().shape)]


class GeoProvider(BaseProvider):  # noqa: D101
    """Geoprovider that selects a concrete implementation class."""

    def geo(self, geo_type: str = "Geometry", crs: CRS = DEFAULT_CRS) -> str:  # noqa: D102
        try:
            wkt = globals()[geo_type]().wkt
        except UnsupportedGEOTypeException as e:
            message = str(e)
            logger.warning("Skipping geo data creation, reason: %s", message)
            return None
        else:
            return f"SRID={crs.srid};{wkt}"


Faker.add_provider(GeoProvider, locale=LOCALE)
