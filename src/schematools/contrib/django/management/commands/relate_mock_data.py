from typing import Any, Dict, List

from schematools.contrib.django.faker.relate import relate_datasets

from . import BaseDatasetCommand


class Command(BaseDatasetCommand):  # noqa: D101
    help = """Relate mock records.

    When mock data is created, the relations are filled with `null` values.
    Using this command, the relations can be added to the records.
    """  # noqa: A003
    requires_system_checks = []

    def handle(self, *args: List[Any], **options: Dict[str, Any]) -> None:  # noqa: D102
        datasets = self.get_datasets(options, enable_db=True)
        relate_datasets(*datasets)
