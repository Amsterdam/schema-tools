from django.contrib.auth.backends import BaseBackend
from schematools.contrib.django.models import (
    Profile,
    generate_permission_key,
    split_permission_key
)

PERMISSION_MATRIX = [
    "read",
    "encoded",
    "random",
]


class ProfileAuthorizationBackend(BaseBackend):
    """
    Handle dataset/table/field/object authorization via single API.
    """

    def get_profiles_for_request(self, request):
        """Get all profiles that match scopes of request.
         """
        if not hasattr(request, "auth_profiles"):
            profiles = []
            for profile in Profile.objects.all():
                scopes = profile.get_scopes()
                if len(scopes) == 0:
                    profiles.append(profile)
                else:
                    if hasattr(request, "is_authorized_for"):
                        if request.is_authorized_for(scopes):
                            profiles.append(profile)
            request.auth_profiles = set(profiles)
        return request.auth_profiles

    def get_all_permissions(self, request):
        """Get all permissions for given request."""
        if not hasattr(request, "auth_permissions"):
            permissions = dict()
            for profile in self.get_profiles_for_request(request):
                profile_permissions = profile.get_permissions()
                permissions = merge_permissions(permissions, profile_permissions)
            request.auth_permissions = permissions
        return request.auth_permissions

    def has_perm(self, user_obj, perm, obj=None):
        """Check if user has permission.
         """
        return self.get_read_permission(
            request=user_obj.request,
            perm=perm,
            obj=obj
        ) is not None

    def get_read_permission(self, request, perm, obj=None):
        """Get permission to read/encode data from profiles."""
        permissions = self.get_all_permissions(request)

        return permissions.get(perm, None)


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
            key=lambda key: PERMISSION_MATRIX.index(key)
        )
    except ValueError as e:
        raise ValueError("Permission {}".format(e.args[0]))
