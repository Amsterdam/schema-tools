from django.apps import apps
from django.contrib.auth.backends import BaseBackend


class ProfileAuthorizationBackend(BaseBackend):
    """
    A Django backend that's used in ``AUTHENTICATION_BACKENDS``.
    This allows model-level permissions based on the
    Handle dataset/table/field/object authorization via single API.
    """

    def has_perm(self, user_obj, perm, obj=None):
        """Check if user has permission."""
        request = user_obj.request
        dataset_id, table_id, field_id = perm.split(":")
        field = apps.get_model(dataset_id, table_id).table_schema().fields[field_id]
        return bool(request.user_scopes.has_field_access(field))
