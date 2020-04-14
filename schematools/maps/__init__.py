from amsterdam_schema.types import DatasetSchema
from . import create


def create_mapfile(dataset: DatasetSchema):
    return create.CreateMapfileFromDataset()(dataset)
