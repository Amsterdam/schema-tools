from __future__ import annotations

from datetime import datetime, tzinfo

from factory import Faker
from faker.generator import Generator
from faker.providers import BaseProvider
from faker.providers.date_time import Provider as DateTimeProvider


class NullableDateTimeProvider(BaseProvider):
    """Provider for a date/date-time or None provider.

    To be used for nullable fields in a database.

    """

    def __init__(self, generator: Generator) -> None:  # noqa: D107
        super().__init__(generator)
        self.date_time = DateTimeProvider(generator)

    def nullable_date_time(  # noqa: D102
        self,
        tzinfo: tzinfo | None = None,
        end_datetime: datetime | None = None,
        nullable=False,
    ) -> datetime | None:

        if nullable and self.generator.random.randint(0, 1):
            return None
        return self.date_time.date_time(
            tzinfo=tzinfo,
            end_datetime=end_datetime,
        )


Faker.add_provider(NullableDateTimeProvider, locale="nl_NL")
