from typing import Protocol

from schematools.domain.graph.objects import DatasetGraph


class DatasetGraphRepository(Protocol):
    def materialize(self) -> "DatasetGraph": ...
