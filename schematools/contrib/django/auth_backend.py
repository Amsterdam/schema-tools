from django.contrib.auth.backends import BaseBackend
from schematools.contrib.django.models import (
    get_active_profiles,
)

PERMISSION_LIST = [
    "read",
    "encoded",
    "random",
    "letter",
]


class RequestProfile(object):
    def __init__(self, request):
        self.request = request
        self.auth_profiles = None
        self.auth_permissions = None

    def get_profiles(self):
        """Get all profiles that match scopes of request.
         """
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

    def get_all_permissions(self):
        """Get all permissions for given request."""
        if self.auth_permissions is None:
            permissions = dict()
            for profile in self.get_profiles():
                profile_permissions = profile.get_permissions()
                permissions = merge_permissions(permissions, profile_permissions)
            self.auth_permissions = permissions
        return self.auth_permissions

    def get_read_permission(self, perm, obj=None):
        """Get permission to read/encode data from profiles."""
        permissions = self.get_all_permissions()
        return permissions.get(perm, None)


class ProfileAuthorizationBackend(BaseBackend):
    """
    Handle dataset/table/field/object authorization via single API.
    """

    def has_perm(self, user_obj, perm, obj=None):
        """Check if user has permission.
         """
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
    """Find highest permission of two.
    """
    try:
        return sorted(
            [permission1, permission2],
            key=lambda key: PERMISSION_LIST.index(key.split(":")[0]),
        )[0]
    except (ValueError, IndexError) as e:
        raise ValueError("Permission {}".format(e.args[0]))
