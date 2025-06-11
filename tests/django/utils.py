from __future__ import annotations

from schematools.contrib.django.factories import DjangoModelFactory


def get_models(dataset):
    factory = DjangoModelFactory(dataset)
    return factory.build_models()
