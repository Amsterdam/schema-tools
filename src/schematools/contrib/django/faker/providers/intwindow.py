import secrets

from factory import Faker
from faker.providers import BaseProvider


class IntWindowProvider(BaseProvider):  # noqa: D101
    """Provider for an integer with an min- and max-value."""

    def int_window(self, min_value: int = 1800, max_value: int = 2000) -> str:  # noqa: D102
        return str(secrets.choice(range(min_value, max_value)))


Faker.add_provider(IntWindowProvider, locale="nl_NL")
