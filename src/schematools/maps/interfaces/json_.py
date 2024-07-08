from __future__ import annotations

import json
import os
import pprint
import typing

import click
import jsonref


class JsonStrategicLoader(jsonref.JsonLoader):
    def __init__(self, strategies: list[typing.Callable], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.strategies = strategies

    def _call_strategy(self, strategy: typing.Callable, uri: str, **load_params):
        return json.loads(strategy(uri), **load_params)

    def _call_parent(self, uri, **load_params):
        return super().__call__(uri, **load_params)

    def __call__(self, uri: str, **load_params):
        for strategy in self.strategies:
            try:
                return self._call_strategy(strategy, uri, **load_params)
            except ValueError:
                pass
        return self._call_parent(uri, **load_params)


def local_strategy(uri: str) -> str:
    if uri.startswith("./"):
        with open(uri) as f:
            return f.read()
    raise ValueError


def load(document_file: typing.IO[str], base_uri: str = "", loader=None, backend=jsonref.load):
    loader = loader or JsonStrategicLoader([local_strategy])
    return backend(document_file, base_uri, loader)


@click.command()
@click.argument("document_path", type=click.types.Path(), required=True)
def main(document_path):
    base_uri = f"file://{os.path.dirname(os.path.realpath(document_path))}/"
    with open(document_path) as document:
        result = load(document, base_uri)
        pprint.pprint(result)
