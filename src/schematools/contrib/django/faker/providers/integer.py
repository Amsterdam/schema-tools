from __future__ import annotations

from factory import Faker
from faker.providers import BaseProvider


class NullableIntegerProvider(BaseProvider):
    """Provider for an integer or None provider.

    To be used for nullable fields in a database.

    A package `faker-optional` exists that has comparable functionality,
    however, the package seems to be abandoned.

    Building a more generic wrapper to make a faker nullable
    for several datatypes turned out to be quite complex
    (especially integrating it in fake/factoryboy).
    """

    def nullable_int(  # noqa: D102
        self, min_: int = 0, max_: int = 9999, step: int = 1, nullable=False
    ) -> int | None:

        if nullable and self.generator.random.randint(0, 1):
            return None
        return self.random_int(min_, max_, step)


Faker.add_provider(NullableIntegerProvider, locale="nl_NL")
