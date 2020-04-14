from amsterdam_schema.types import DatasetSchema
from .generators.mapfile import MapfileGenerator

# XXX see generators/mapfile.py about this import
from .generators.schema import Dataset
from .interfaces.mapfile.serializers import MappyfileSerializer


class CreateMapfileFromDataset:
    """ Creates a Mapfile from a dataset in JSON """

    _generator = MapfileGenerator(serializer=MappyfileSerializer())

    def __call__(self, dataset: DatasetSchema):
        return self._generator(dataset)
