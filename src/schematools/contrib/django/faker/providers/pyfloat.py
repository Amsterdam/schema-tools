from __future__ import annotations

from factory import Faker
from faker.generator import Generator
from faker.providers import BaseProvider
from faker.providers.python import Provider as PyProvider


class NullableFloatProvider(BaseProvider):
    """Provider for a float or None provider.

    To be used for nullable fields in a database.

    """

    def __init__(self, generator: Generator) -> None:
        """Declare the other faker providers."""
        super().__init__(generator)
        self.python = PyProvider(generator)

    def nullable_float(  # noqa: D102
        self,
        left_digits=None,
        right_digits=None,
        positive=False,
        min_value=None,
        max_value=None,
        nullable=False,
    ) -> float | None:

        if nullable and self.generator.random.randint(0, 1):
            return None

        return self.python.pyfloat(
            left_digits=left_digits,
            right_digits=right_digits,
            positive=positive,
            min_value=min_value,
            max_value=max_value,
        )


Faker.add_provider(NullableFloatProvider, locale="nl_NL")
