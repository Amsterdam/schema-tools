from __future__ import annotations

import enum
from dataclasses import InitVar, dataclass, field


class LayerType(str, enum.Enum):
    point = "POINT"
    polygon = "POLYGON"
    # TODO add otehr layer types here


class Metadata(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["__type__"] = "metadata"


@dataclass
class Connection:
    type: str
    data: str

    def __init__(self, type, s):
        self.type = type
        self.data = s

    @classmethod
    def for_postgres(cls, user, pw, dbname, host):
        return cls("postgis", f"user={user} password={pw} dbname={dbname} host={host}")

    def __str__(self):
        return self.data


class Data(str):
    @classmethod
    def for_postgres(cls, column, table, srid=None, UNIQUE=None):
        result = cls(f"{column} from {table}")
        if srid:
            result += f" USING srid={srid}"
        if UNIQUE:
            result += f" USING UNIQUE {UNIQUE}"
        return result


@dataclass
class FeatureClass:
    """Used for rendering a feature"""

    name: str | None = None
    expression: str | None = None
    # https://mapserver.org/mapfile/style.html#style
    styles: list[dict] = field(default_factory=list)
    # https://mapserver.org/mapfile/label.html#label
    labels: list[dict] = field(default_factory=list)

    __type__: str = field(init=False, default="class")

    def add_style(self, d):
        self.styles.append({"__type__": "style", **d})

    def add_label(self, d):
        self.labels.append({"__type__": "label", **d})


Filename = str


@dataclass
class Layer:
    name: str
    type: str  # TODO: get this type from schema
    with_connection: InitVar[Connection] = None
    projection: list[str] | None = None

    connection: str | None = field(init=False, default=None)
    connectiontype: str | None = field(init=False, default=None)

    data: list[Data] = field(default_factory=list)
    classes: list[FeatureClass] = field(default_factory=list)

    include: list[Filename] = field(default_factory=list)
    labelitem: str | None = None
    metadata: Metadata = field(default_factory=Metadata)

    __type__: str = field(init=False, default="layer")

    def __post_init__(self, connection: Connection = None):
        if connection:
            self.connection = connection.data
            self.connectiontype = connection.type


@dataclass
class Web:
    metadata: Metadata = field(default_factory=Metadata)
    __type__: str = field(init=False, default="web")


@dataclass
class Mapfile:
    name: str
    layers: list[Layer]
    projection: list[str] | None = None
    include: list[Filename] = field(default_factory=list)
    web: Web | None = None
