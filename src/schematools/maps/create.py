from __future__ import annotations

from schematools.types import DatasetSchema

from .generators.mapfile import MapfileGenerator

# XXX see generators/mapfile.py about this import
from .interfaces.mapfile.serializers import MappyfileSerializer


def create_map_from_dataset(dataset: DatasetSchema):
    """Creates a Mapfile from a dataset in JSON"""

    _generator = MapfileGenerator(serializer=MappyfileSerializer())
    return _generator(dataset)
