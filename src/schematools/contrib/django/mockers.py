"""The Django ModelFactories for Amsterdam Schema mock data.

When ModelFactories are generated with :func:`~schematools.contrib.django.factories.model_mocker_factory`,
they all inherit from :class:`~schematools.contrib.django.mockers.DynamicModelFactory` to have
a common interface.

Note that the Models and ModelFactories are bootstrapped from `~schematools.contrib.django.factories.py`,
so to circumvent this naming clash, we place the ModelFactory logic in `~schematools.contrib.django.mockers.py`
as the primary aim of the ModelFactory is to generate Mock data for development and testing purposes.
"""

from factory.django import DjangoModelFactory

from schematools.contrib.django.models import Dataset
from schematools.types import DatasetTableSchema


class DynamicModelMocker(DjangoModelFactory):
    """Base class to tag and detect dynamically generated model mockers."""

    pass
