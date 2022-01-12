from django.db.models import QuerySet


class DatasetQuerySet(QuerySet):
    """Extra ORM methods for the Dataset model."""

    def db_enabled(self):
        """Return all datasets for which models should be created."""
        return self.filter(enable_db=True)

    def endpoint_enabled(self):
        """Return the datasets where data is retrieved from remote API's."""
        return self.filter(enable_db=False, endpoint_url__isnull=False)

    def api_enabled(self):
        """Return only datasets that should get an API."""
        return self.filter(enable_api=True)
