from collections import UserDict


class Dataset(UserDict):
    DEFAULT_CRS = "EPSG:28992"

    @property
    def name(self):
        return self['id']

    @property
    def crs(self) -> str:
        return self.get('crs', self.DEFAULT_CRS)

    @property
    def classes(self):
        return [
            Dataclass(i) for i in self['classes']
        ]


class Dataclass(UserDict):
    pass
