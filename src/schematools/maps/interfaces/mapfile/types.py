import enum
import typing
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

    name: typing.Optional[str] = None
    expression: typing.Optional[str] = None
    # https://mapserver.org/mapfile/style.html#style
    styles: typing.List[dict] = field(default_factory=list)
    # https://mapserver.org/mapfile/label.html#label
    labels: typing.List[dict] = field(default_factory=list)

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
    projection: typing.Optional[typing.List[str]] = None

    connection: typing.Optional[str] = field(init=False, default=None)
    connectiontype: typing.Optional[str] = field(init=False, default=None)

    data: typing.List[Data] = field(default_factory=list)
    classes: typing.List[FeatureClass] = field(default_factory=list)

    include: typing.List[Filename] = field(default_factory=list)
    labelitem: typing.Optional[str] = None
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
    layers: typing.List[Layer]
    projection: typing.Optional[typing.List[str]] = None
    include: typing.List[Filename] = field(default_factory=list)
    web: typing.Optional[Web] = None
