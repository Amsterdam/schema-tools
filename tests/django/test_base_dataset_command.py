from __future__ import annotations

from dataclasses import dataclass

import pytest
from django.conf import settings
from django.core.management import CommandError

from schematools.contrib.django.management.commands import BaseDatasetCommand


@dataclass
class DatasetStub:
    name: str
    enable_db: bool | None = None


class QuerySetStub:
    def __init__(self, datasets: list[DatasetStub]):
        self._datasets = list(datasets)

    def all(self):
        return QuerySetStub(self._datasets)

    def filter(self, **kwargs):
        datasets = self._datasets
        if "enable_db" in kwargs:
            datasets = [ds for ds in datasets if ds.enable_db == kwargs["enable_db"]]
        if "name__in" in kwargs:
            allowed = set(kwargs["name__in"])
            datasets = [ds for ds in datasets if ds.name in allowed]
        return QuerySetStub(datasets)

    def __iter__(self):
        return iter(self._datasets)


def test_add_arguments_uses_dataset_settings_defaults(monkeypatch):
    monkeypatch.setattr(settings, "DATASETS_LIST", ["alpha", "beta"], raising=False)
    monkeypatch.setattr(settings, "DATASETS_EXCLUDE", ["skip_me"], raising=False)

    parser = BaseDatasetCommand().create_parser("manage.py", "datasets")
    options = vars(parser.parse_args(["chosen", "--exclude", "ignored"]))

    assert options["dataset"] == ["chosen"]
    assert options["datasets_list"] == ["alpha", "beta"]
    assert options["datasets_exclude"] == ["ignored"]


def test_get_datasets_requires_names_or_exclusions(monkeypatch):
    monkeypatch.setattr(
        "schematools.contrib.django.management.commands.Dataset.objects",
        QuerySetStub([]),
    )

    with pytest.raises(CommandError, match="Provide at least a dataset"):
        BaseDatasetCommand().get_datasets(
            {"dataset": [], "datasets_list": None, "datasets_exclude": None}
        )


def test_get_datasets_prefers_positional_names_and_applies_filters(monkeypatch):
    datasets = [
        DatasetStub("alpha", enable_db=True),
        DatasetStub("beta", enable_db=True),
        DatasetStub("gamma", enable_db=False),
    ]
    monkeypatch.setattr(
        "schematools.contrib.django.management.commands.Dataset.objects",
        QuerySetStub(datasets),
    )
    options = {
        "dataset": ["beta", "gamma"],
        "datasets_list": ["alpha"],
        "datasets_exclude": ["gamma"],
    }

    queryset = BaseDatasetCommand().get_datasets(options, enable_db=True)

    assert options["datasets_list"] == ["beta", "gamma"]
    assert [dataset.name for dataset in queryset] == ["beta"]


def test_get_datasets_default_all_uses_all_datasets(monkeypatch):
    datasets = [DatasetStub("alpha"), DatasetStub("beta")]
    monkeypatch.setattr(
        "schematools.contrib.django.management.commands.Dataset.objects",
        QuerySetStub(datasets),
    )

    queryset = BaseDatasetCommand().get_datasets(
        {"dataset": [], "datasets_list": None, "datasets_exclude": None},
        default_all=True,
    )

    assert [dataset.name for dataset in queryset] == ["alpha", "beta"]


def test_get_datasets_raises_for_invalid_names(monkeypatch):
    monkeypatch.setattr(
        "schematools.contrib.django.management.commands.Dataset.objects",
        QuerySetStub([DatasetStub("alpha")]),
    )

    with pytest.raises(CommandError, match="Datasets not found: beta"):
        BaseDatasetCommand().get_datasets(
            {"dataset": [], "datasets_list": ["alpha", "beta"], "datasets_exclude": None}
        )
