from django.contrib.auth.backends import BaseBackend
from schematools.contrib.django.models import (
    Dataset,
    DatasetTable,
    get_active_profiles,
    generate_permission_key,
)

# In order of importance, first one overrules the lower one
PERMISSION_LIST = [
    "read",
    "encoded",
    "random",
    "letters",
]


class RequestProfile(object):
    def __init__(self, request):
        self.request = request
        self.auth_profiles = None
        self.auth_permissions = None

    def get_profiles(self):
        """Get all profiles that match scopes of request."""
        if self.auth_profiles is None:
            profiles = []
            for profile in get_active_profiles():
                scopes = profile.get_scopes()
                if len(scopes) == 0:
                    profiles.append(profile)
                else:
                    if hasattr(self.request, "is_authorized_for"):
                        if self.request.is_authorized_for(*scopes):
                            profiles.append(profile)
            self.auth_profiles = set(profiles)
        return self.auth_profiles

    def get_all_permissions(self, perm):
        """Get all permissions for given request."""
        if self.auth_permissions is None:
            permissions = dict()
            dataset_id, table_id, field_id = perm.split(":")
            dataset = Dataset.objects.get(name=dataset_id)
            has_dataset_scope = self.request.is_authorized_for(dataset.auth)
            table = DatasetTable.objects.get(dataset=dataset, name=table_id)
            for field in table.fields.all():
                permission_key = generate_permission_key(
                    dataset_id, table_id, field.name
                )
                if has_dataset_scope:
                    permissions[permission_key] = PERMISSION_LIST[
                        0
                    ]  # get the top permission
                else:
                    has_table_scope = self.request.is_authorized_for(table.auth)
                    if has_table_scope:
                        permissions[permission_key] = PERMISSION_LIST[
                            0
                        ]  # get the top permission
            for profile in self.get_active_profiles(dataset_id, table_id):
                profile_permissions = profile.get_permissions()
                permissions = merge_permissions(permissions, profile_permissions)
            self.auth_permissions = permissions
        return self.auth_permissions

    def get_read_permission(self, perm, obj=None):
        """Get permission to read/encode data from profiles."""
        permissions = self.get_all_permissions(perm)
        return permissions.get(perm, None)

    def get_active_profiles(self, dataset_id, table_id):
        """Returns the profiles that 1) are relevant to the table
        and 2) have met their filterset obligations"""
        profiles = []
        for profile in self.get_profiles():
            relevant_dataset_schema = profile.schema.datasets.get(dataset_id, None)
            if relevant_dataset_schema:
                relevant_table_schema = relevant_dataset_schema.tables.get(
                    table_id, None
                )
                if relevant_table_schema:
                    if relevant_table_schema.mandatory_filterset_obligation_fulfilled(
                        self.request
                    ):
                        profiles.append(profile)
        return profiles


class ProfileAuthorizationBackend(BaseBackend):
    """
    Handle dataset/table/field/object authorization via single API.
    """

    def has_perm(self, user_obj, perm, obj=None):
        """Check if user has permission."""
        request = user_obj.request

        if not hasattr(request, "auth_profile"):
            request.auth_profile = RequestProfile(request)

        return request.auth_profile.get_read_permission(perm=perm, obj=obj) is not None


def merge_permissions(base_permissions, profile_permissions):
    """Merge permissions recursively."""
    for key, value in profile_permissions.items():
        if key in base_permissions:
            if isinstance(value, str):
                base_permissions[key] = highest_permission(value, base_permissions[key])
            else:
                base_permissions[key] = merge_permissions(base_permissions[key], value)
        else:
            base_permissions[key] = value
    return base_permissions


def highest_permission(permission1, permission2):
    """Find highest permission of two."""
    try:
        return sorted(
            [permission1, permission2],
            key=lambda key: PERMISSION_LIST.index(key.split(":")[0]),
        )[0]
    except (ValueError, IndexError) as e:
        raise ValueError(f"Permission {e.args[0]}") from None
