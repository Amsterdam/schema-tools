from schematools.types import DatasetSchema

from . import create


def create_mapfile(dataset: DatasetSchema):
    return create.create_map_from_dataset(dataset)
