from dataclasses import asdict, dataclass

import mappyfile

#  Serializing with Jinja templates
from jinja2 import Environment, FileSystemLoader

from . import types


def schema_dict(items):
    return {k: v for k, v in items if v is not None}


@dataclass
class MappyfileSerializer:
    template: str = "MAP END"

    def map_file_to_dict(self, map_file):
        return asdict(map_file, dict_factory=schema_dict)

    def __call__(self, map_file: types.Mapfile):
        backend_file = mappyfile.loads(self.template)
        backend_file.update(self.map_file_to_dict(map_file))
        errors = mappyfile.validate(backend_file)
        if errors:
            raise Exception(errors)
        return mappyfile.dumps(backend_file)


@dataclass
class JinjaSerializer:
    template_dir: str

    @property
    def env(self) -> Environment:
        env = Environment(loader=FileSystemLoader(self.template_dir))
        env.trim_blocks = True
        env.lstrip_blocks = True
        return env

    def __call__(self, template_file_name: str, context: dict):
        template = self.env.get_template(template_file_name)
        return template.render(**context)
