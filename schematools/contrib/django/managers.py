from django.db.models import QuerySet, Manager

class DatasetQuerySet(QuerySet):
    """Extra ORM methods for the Dataset model."""

    def db_enabled(self):
        """Return all datasets for which models should be created"""
        return self.filter(enable_db=True)

    def endpoint_enabled(self):
        """Return the datasets for which"""
        return self.filter(enable_db=False, endpoint_url__isnull=False)

    def api_enabled(self):
        return self.filter(enable_api=True)


    def hallo(self):
        return "hallo"


class LooseRelationsManager(Manager):
    def get_queryset(self):
        # Hier logica om te dealen met loose relations
        qs = super().get_queryset()
        return qs
