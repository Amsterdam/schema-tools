"""The Django ModelFactories for Amsterdam Schema mock data.

When ModelFactories are generated with
:func:`~schematools.contrib.django.factories.model_mocker_factory`,
they all inherit from
:class:`~schematools.contrib.django.mockers.DynamicModelFactory`
to have a common interface.

Note that the Models and ModelFactories are bootstrapped from
`~schematools.contrib.django.factories.py`, so to circumvent this naming clash,
we place the ModelFactory logic in `~schematools.contrib.django.mockers.py`
as the primary aim of the ModelFactory is to generate Mock data for development
and testing purposes.
"""

from __future__ import annotations

from factory.django import DjangoModelFactory


class DynamicModelMocker(DjangoModelFactory):
    """Base class to tag and detect dynamically generated model mockers."""

    @classmethod
    def _setup_next_sequence(cls):
        # Instead of defaulting to starting at 0, we start at > 0
        # this is more appropriate for database keys
        # NB This classmethod can be replaced in the dango mgm command
        # to be able to start at a higher sequence number.
        return 1
