from __future__ import annotations

from typing import Any

from schematools.contrib.django.faker.relate import relate_datasets

from . import BaseDatasetCommand


class Command(BaseDatasetCommand):  # noqa: D101
    help = """Relate mock records.

    When mock data is created, the relations are filled with `null` values.
    Using this command, the relations can be added to the records.
    """  # noqa: A003
    requires_system_checks = []

    def handle(self, *args: list[Any], **options: dict[str, Any]) -> None:  # noqa: D102
        datasets = self.get_datasets(options, enable_db=True)
        relate_datasets(*datasets)
