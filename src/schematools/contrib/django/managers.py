from __future__ import annotations

from django.db.models import QuerySet
from django.db.models.query import ModelIterable


class DatasetIterable(ModelIterable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from .loaders import DatabaseSchemaLoader

        self._loader = DatabaseSchemaLoader()

    def __iter__(self):
        """Inject a shared dataset collection with the results of this queryset."""
        for obj in super().__iter__():
            obj._loader = self._loader
            yield obj


class DatasetQuerySet(QuerySet):
    """Extra ORM methods for the Dataset model.

    All datasets that were retrieved with the same queryset are linked together,
    so they can resolve relations between each other.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._iterable_class = DatasetIterable

    def db_enabled(self):
        """Return all datasets for which models should be created."""
        return self.filter(enable_db=True)

    def api_enabled(self):
        """Return only datasets that should get an API."""
        return self.filter(enable_api=True)
