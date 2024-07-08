from __future__ import annotations

from collections import ChainMap
from dataclasses import dataclass

from schematools.maps.interfaces.mapfile import serializers, types
from schematools.types import DatasetSchema, DatasetTableSchema

MapfileStr = str


@dataclass
class MapserviceLayerContext:
    """Context for generating mapfile layers from a amsterdam datatable
    and its parent dataset."""

    dataset: DatasetSchema
    datatable: DatasetTableSchema

    @property
    def name(self):
        return self.datatable.id

    @property
    def projections(self) -> list[str]:
        return [self.dataset["crs"]]

    @property
    def srid(self) -> int:
        return int(self.dataset["crs"].split(":")[-1])  # XXX replace with our impl.

    @property
    def metadata(self) -> dict:
        return {"ows_enable_request": "*"}


class MapServiceContext:
    """Context for generating mapfiles from a amsterdam dataset"""

    def __init__(self, dataset: DatasetSchema):
        self.dataset = dataset

    @property
    def name(self):
        return self.dataset.id

    @property
    def layers(self) -> list[MapserviceLayerContext]:
        return [MapserviceLayerContext(self.dataset, dt) for dt in self.dataset.tables]


@dataclass
class Generator:
    def serialize(self, dataset) -> str:
        raise NotImplementedError

    def __call__(self, dataset):
        return self.serialize(dataset)


@dataclass
class MapfileGenerator(Generator):
    serializer: serializers.MappyfileSerializer

    def generate_feature_class(self, feature_dict, base_styles=None) -> types.FeatureClass:
        feature_class = types.FeatureClass(
            name=feature_dict["name"], expression=feature_dict.get("expression")
        )
        styles = feature_dict.get("styles", base_styles)

        for index, style in enumerate(styles):
            base_style: dict = {}
            try:
                base_style = base_styles[index]
            except IndexError:
                pass
            feature_class.add_style(ChainMap(style, base_style))
        return feature_class

    def generate_layer(self, context: MapserviceLayerContext) -> types.Layer:
        return types.Layer(
            name=context.name,
            type=types.LayerType.polygon,
            with_connection=types.Connection.for_postgres("postgres", "", "postgres", "postgres"),
            data=[
                types.Data.for_postgres(
                    "geometry",
                    f"{context.dataset.id}.{context.name}",
                    srid=context.srid,
                    UNIQUE="id",
                )
            ],
            classes=[
                types.FeatureClass(
                    styles=[{"__type__": "style", "color": [200, 50, 50], "antialias": True}]
                )
            ],
            metadata=types.Metadata(context.metadata),
        )

    def serialize(self, dataset: DatasetSchema) -> MapfileStr:
        context = MapServiceContext(dataset)
        mapfile = types.Mapfile(name=context.name, layers=[], include=["header.inc"])
        mapfile.layers.extend(map(self.generate_layer, context.layers))
        return MapfileStr(self.serializer(mapfile))


@dataclass
class LegacyMapfileGenerator(Generator):
    serializer: serializers.JinjaSerializer

    def serialize(self, dataset) -> str:
        ds_dict = dataset.__dict__

        # get layers
        ds_dict["layers"] = (x.__dict__ for x in dataset.maplayer_set.all())
        ds_dict["id_field"] = dataset.pk_field

        template_file = "default.map.template"
        if dataset.map_template:
            template_file = dataset.map_template

        return self.serializer(template_file, context={"ds": ds_dict})
