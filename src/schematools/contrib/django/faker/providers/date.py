from __future__ import annotations

from datetime import date, datetime

from factory import Faker
from faker.generator import Generator
from faker.providers import BaseProvider
from faker.providers.date_time import Provider as DateTimeProvider


class NullableDateProvider(BaseProvider):
    """Provider for a date or None provider.

    To be used for nullable fields in a database.

    """

    def __init__(self, generator: Generator) -> None:  # noqa: D107
        super().__init__(generator)
        self.date_time = DateTimeProvider(generator)

    def nullable_date_object(  # noqa: D102
        self,
        end_datetime: datetime | None = None,
        nullable=False,
    ) -> date | None:

        if nullable and self.generator.random.randint(0, 1):
            return None
        return self.date_time.date_object(
            end_datetime=end_datetime,
        )


Faker.add_provider(NullableDateProvider, locale="nl_NL")
