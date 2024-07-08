from __future__ import annotations

from factory import Faker
from faker.providers import BaseProvider


class NullerProvider(BaseProvider):  # noqa: D101
    """Provider for an null value."""

    def nuller(self) -> None:  # noqa: D102
        return None


Faker.add_provider(NullerProvider, locale="nl_NL")
