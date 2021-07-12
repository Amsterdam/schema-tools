"""Authorization ruleset handling.

The :class:`UserScopes` class handles whether a dataset, table or field can be accessed.
The other classes in this module ease to retrieval of permission objects.
"""
from __future__ import annotations

from functools import wraps
from typing import Callable, Iterable, List, Optional

import methodtools

from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    Permission,
    PermissionLevel,
    ProfileDatasetSchema,
    ProfileSchema,
    ProfileTableSchema,
)

HasAllScopesFunc = Callable[..., bool]  # can't describe func(*scopes: str) -> bool
empty_dict = {}

__all__ = (
    "UserScopes",
    "create_scopes_check",
)


def create_scopes_check(*user_scopes: str) -> HasAllScopesFunc:
    """Create a function that tests whether the scopes are set.
    This function only needs to be used when features such as
    ``request.is_authorized_for(*scopes)`` from authorization_django are not available.
    """
    user_scopes = set(user_scopes)  # can be empty
    return lambda *needed: user_scopes.issuperset(needed)


def abort_on_highest(func):
    """Decorator to abort searching for permissions when the highest value was found."""

    @wraps(func)
    def _abort_on_highest(*args, **kwargs) -> Permission:
        try:
            return func(*args, **kwargs)
        except HighestPermissionFound as e:
            return e.permission

    return _abort_on_highest


class HighestPermissionFound(Exception):
    def __init__(self, permission: Permission):
        self.permission = permission


class PermissionBag:
    """A helper class to ease collecting permissions from profile objects.
    This reduces the DRY code in permission searches, when used together with ``abort_on_highest``.

    It helps to find the highest permission level,
    and automatically aborts when the highest possible value is found.
    """

    def __init__(self):
        self._collection = []

    def append(self, permission: Permission):
        """Add a new discovered permission level to this collection."""
        self._collection.append(permission)
        if permission.level is PermissionLevel.highest:
            # Avoid further code flow.
            raise HighestPermissionFound(permission)

    @property
    def highest_value(self) -> Permission:
        """Return the highest found permission."""
        return max(self._collection, default=Permission.none)

    def __bool__(self):
        return bool(self._collection)


class UserScopes:
    """A request-like object that tells what the current user may access.
    This is the foundation for all permission checks.

    All ``has_...()`` functions are used for permission checks.
    Internally, these read the schema and profile data for the authorization matrix.

    * By default, all fields can be read unless the schema defines an "auth" field.
    * The "auth" flags in schema files act as a blacklist: no access, except for some roles.
    * The "profile" rules open up certain fields, hence whitelist features.
    """

    def __init__(
        self,
        query_params: dict[str, ...],
        is_authorized_for: HasAllScopesFunc,
        all_profiles: Optional[Iterable[ProfileSchema]] = None,
    ):
        """
        :param query_params: The search query filter (e.g. request.GET).
        :param is_authorized_for: A function that checks for authorization of scopes
            For example ``request.is_authorized_for()`` from ``authorization_django``
            or use :func:`create_scopes_check` as a substitute.
        :param all_profiles: All profiles that need to be loaded.
        """
        self._query_param_names = [param for param, value in query_params.items() if value]
        self._all_profiles = all_profiles
        self._is_authorized_for = is_authorized_for

    def add_query_params(self, params: List[str]):
        """Tell that the request has extra (implicit) parameters that are satisfied, e.g. via the URL."""
        self._query_param_names.extend(params)

    @methodtools.lru_cache()
    def has_all_scopes(self, *needed_scopes: str) -> bool:
        """Check whether the request has all scopes.
        Note: at ``__init__()``, this function receives an LRU-cache for performance.
        """
        return self._is_authorized_for(*needed_scopes)

    @methodtools.lru_cache()
    def has_any_scope(self, *needed_scopes: str) -> bool:
        """Check whether the request grants one of the given scopes.
        This performs an OR check: having one of the scopes gives access.
        Note: at ``__init__()``, this function receives an LRU-cache for performance.
        """
        return not needed_scopes or any(
            self.has_all_scopes(needed_scope) for needed_scope in needed_scopes
        )

    def has_dataset_access(self, dataset: DatasetSchema) -> Permission:
        """Tell whether a dataset can be accessed"""
        return self._has_dataset_auth_access(dataset) or self._has_dataset_profile_access(
            dataset.id
        )

    def has_table_access(self, table: DatasetTableSchema) -> Permission:
        """Tell whether a table can be accessed, and return the permission level"""
        # When the user has an "auth" scope, they may always enter.
        # Otherwise, the user can only enter when the required profile rules are satisfied,
        # which includes mandatory filtersets.
        return self._has_table_auth_access(table) or self._has_table_profile_access(table)

    def has_field_access(self, field: DatasetFieldSchema) -> Permission:
        """Tell whether a field may be read."""
        # Again, when a field "auth" scope is satisfied, no further checks are done.
        # Otherwise, the field + table rules are checked from the profile.
        return self._has_field_auth_access(field) or self._has_field_profile_access(field)

    def _has_dataset_auth_access(self, dataset: DatasetSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the dataset."""
        if self.has_any_scope(*dataset.auth):
            return Permission(PermissionLevel.highest, source="dataset.auth")
        else:
            return Permission.none

    def _has_table_auth_access(self, table: DatasetTableSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the table."""
        if self.has_any_scope(*table.auth) and self.has_any_scope(*table.dataset.auth):
            return Permission(
                PermissionLevel.highest, source="table.auth" if table.auth else "dataset.auth"
            )
        else:
            return Permission.none

    def _has_field_auth_access(self, field: DatasetFieldSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the table."""
        if (
            self.has_any_scope(*field.auth)
            and self.has_any_scope(*field.table.auth)
            and self.has_any_scope(*field.table.dataset.auth)
        ):
            return Permission(
                PermissionLevel.highest,
                source=(
                    "field.auth"
                    if field.auth
                    else ("table.auth" if field.table.auth else "dataset.auth")
                ),
            )
        else:
            return Permission.none

    @methodtools.lru_cache()
    def _has_dataset_profile_access(self, dataset_id: str) -> Permission:
        """Give the permission access level for a dataset, as defined by the profile."""
        return max(
            (
                profile_dataset.permissions
                for profile_dataset in self.get_active_profile_datasets(dataset_id)
            ),
            default=Permission.none,
        )

    @methodtools.lru_cache()
    @abort_on_highest
    def _has_table_profile_access(self, table: DatasetTableSchema) -> Permission:
        """Give the permission level for a table.

        When a dataset defines global permissions without explicitly mentioning the table,
        these permissions are "inherited" and used.
        """
        dataset_id = table.dataset.id
        table_id = table.id
        permissions = PermissionBag()

        for profile_dataset in self.get_active_profile_datasets(dataset_id):
            # If a profile defines "read" on the whole dataset, without explicitly
            # mentioning a table, this means the table can also be read unconditionally.
            profile_table = profile_dataset.tables.get(table_id, None)
            if profile_table is None and (dataset_permission := profile_dataset.permissions):
                permissions.append(dataset_permission)

            # Otherwise see if the table can be included (mandatory filters match)
            if self._may_include_profile_table(profile_table):
                permissions.append(profile_table.permissions)

        # Datasets may a permission that also covers this table,
        # but tables could also define an explicit permission. See who wins.
        return permissions.highest_value

    @abort_on_highest
    def _has_field_profile_access(self, field: DatasetFieldSchema) -> Permission:
        """Give the permission level for a field based on a profile

        Fields have a special case: if a specific permission is defined, use that.
        This may "limit" the actual permission. For example, the table gives "read" permission,
        but the field may state "encoded" as the level. Since a default is defined for the field,
        that's being used.
        """
        permissions = PermissionBag()
        field_id = field.id
        table_id = field.table.id

        # First see if there is an explicit definition for a field:
        for profile_dataset in self.get_active_profile_datasets(field.table.dataset.id):
            # If a profile defines "read" on the whole dataset, without explicitly
            # mentioning the table, this means the table can also be read unconditionally.
            profile_table = profile_dataset.tables.get(table_id, None)
            if profile_table is None:
                if dataset_permission := profile_dataset.permissions:
                    permissions.append(dataset_permission)
                continue

            # See if the table can be included (mandatory filters match)
            if not self._may_include_profile_table(profile_table):
                continue

            # See if the table defines the current field
            try:
                field_permission = profile_table.fields[field_id]
            except KeyError:
                # No explicit field defined, consider table as fallback
                # Some tables define global permissions without mentioning the field.
                # These get preference over explicit field permissions from other profiles.
                # When we only find profiles that mention the field explicitly,
                # that highest level will be returned instead.
                table_permission = profile_table.permissions
                if table_permission and table_permission.level > PermissionLevel.subobjects_only:
                    permissions.append(table_permission)
                continue

            permissions.append(field_permission)

        return permissions.highest_value

    @methodtools.lru_cache()
    def get_active_profile_datasets(self, dataset_id: str) -> List[ProfileDatasetSchema]:
        """Find all profiles that mention a dataset and match the scopes.
        This already checks whether the mandatory user scopes are set.
        """
        if self._all_profiles is None:
            raise RuntimeError("load_profiles() is not called yet")
        elif not isinstance(self._all_profiles, list):
            self._all_profiles = list(self._all_profiles)  # perform query on demand.

        return [
            profile_dataset
            for profile in self._all_profiles
            # Profiles are only activated when:
            # - ALL scopes are matched
            # - dataset is mentioned in the profile
            if self.has_any_scope(*profile.scopes)  # TODO: this mimics the current behavior!
            and (profile_dataset := profile.datasets.get(dataset_id)) is not None
        ]

    @methodtools.lru_cache()
    def get_active_profile_tables(
        self, dataset_id: str, table_id: str
    ) -> List[ProfileTableSchema]:
        """Internal _get_active_profile_tables() variant to cache by string-keys."""
        return [
            profile_table
            for profile_dataset in self.get_active_profile_datasets(dataset_id)
            # Profiles are only activated when:
            # - table is mentioned in the profile
            # - ALL scopes are matched (tested for dataset already)
            # - ONE mandatory filter is matched (if filters are required)
            if (profile_table := profile_dataset.tables.get(table_id)) is not None
            and self._may_include_profile_table(profile_table)
        ]

    def _may_include_profile_table(self, profile_table: ProfileTableSchema):
        """Check whether the table rules are applicable to the current user.
        This checks whether any of the mandatory filtersets from a ProfileTableSchema were queried.
        """
        mandatory_filtersets = profile_table.mandatory_filtersets

        # Table is OK when:
        # - there are no mandatory filter sets
        # - one of the rules is matched.
        return not mandatory_filtersets or any(
            _match_filter_rule(rule, self._query_param_names) for rule in mandatory_filtersets
        )


def _match_filter_rule(rule: Iterable[str], query_param_names: Iterable[str]) -> bool:
    """Tell whether a mandatory filter rule is matched.
    This happens when ALL required filters are present in the query string.
    """
    return all(filter_name in query_param_names for filter_name in rule)
