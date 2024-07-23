"""Authorization ruleset handling.

The :class:`UserScopes` class handles whether a dataset, table or field can be accessed.
The other classes in this module ease to retrieval of permission objects.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from schematools._utils import cached_method
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

PUBLIC_SCOPE = "OPENBAAR"

__all__ = ("UserScopes",)


class UserScopes:
    """A request-like object that tells what the current user may access.

    A UserScopes encapsulates the scopes on a request and the profiles that apply,
    and performs permission checks against a schema.

    All ``has_...()`` functions are used for permission checks.
    Internally, these read the schema and profile data for the authorization matrix.

    * By default, all fields can be read unless the schema defines an "auth" field.
    * The "auth" flags in schema files act as a blacklist: no access, except for some roles.
    * The "profile" rules open up certain fields, hence whitelist features.
    """

    def __init__(
        self,
        query_params: dict[str, object],
        request_scopes: Iterable[str],
        all_profiles: Iterable[ProfileSchema] | None = None,
    ):
        """Initialize the user scopes object.

        Args:
            query_params: The search query filter (e.g. request.GET).
            request_scopes: The scopes granted to a request.
                Presence of the public scope "OPENBAAR" is implied.
            all_profiles: All profiles that need to be loaded.
                If not None, this iterable is stored and converted to list
                the first time it is needed.
        """
        self._query_param_names = [param for param, value in query_params.items() if value]
        self._all_profiles = all_profiles
        self._scopes = set(request_scopes) | {PUBLIC_SCOPE}

    def __repr__(self):
        return f"<UserScopes: {self._scopes!r}>"

    def __iter__(self) -> Iterator[str]:
        return iter(self._scopes)

    def add_query_params(self, params: list[str]):
        """Tell that the request has extra (implicit) parameters that are satisfied.

        For example, the detail URL of a resource already implicitly passes the
        identifier of a resource. Hence, this parameter no longer needs to be given
        found in any additional search filters or query string.
        """
        self._query_param_names.extend(params)

    @cached_method()  # type: ignore[misc]
    def has_all_scopes(self, needed_scopes: frozenset[str]) -> bool:
        """Check whether the request has all scopes.

        This performs an AND check: all scopes should be present.
        """
        return self._scopes.issuperset(needed_scopes)

    @cached_method()  # type: ignore[misc]
    def has_any_scope(self, needed_scopes: frozenset[str]) -> bool:
        """Check whether the request grants one of the given scopes.

        This performs an OR check: having one of the scopes gives access.
        """
        return not self._scopes.isdisjoint(needed_scopes)

    def has_dataset_access(self, dataset: DatasetSchema) -> Permission:
        """Tell whether a dataset can be accessed."""
        return self._has_dataset_auth_access(dataset) or self._has_dataset_profile_access(
            dataset.id
        )

    def has_table_access(self, table: DatasetTableSchema) -> Permission:
        """Tell whether a table can be accessed, and return the permission level."""
        # When the user has an "auth" scope, they may always enter.
        # Otherwise, the user can only enter when the required profile rules are satisfied,
        # which includes mandatory filtersets.
        return self._has_table_auth_access(table) or self._has_table_profile_access(table)

    def has_table_fields_access(self, table: DatasetTableSchema) -> bool:
        """Tell whether all fields of a table can be accessed."""
        for field in table.fields:  # noqa: SIM110 (needs optimized for statement instead of all())
            if not self.has_field_access(field):
                return False

        return True

    def has_field_access(self, field: DatasetFieldSchema) -> Permission:
        """Tell whether a field may be read."""
        # Again, when a field "auth" scope is satisfied, no further checks are done.
        # Otherwise, the field + table rules are checked from the profile.
        return self._has_field_auth_access(field) or self._has_field_profile_access(field)

    def has_field_filter_access(self, field: DatasetFieldSchema) -> Permission:
        """Tell whether a field may be used in searching.

        Some fields do not allow filtering the data for privacy reasons.
        An example is filtering all buildings by owner (hence knowing their portfolio),
        while the user is still allowed to see the owner of each individual building.

        This also checks whether the field has read access (:meth:`has_field_access`).
        """
        # There is no profile option yet for profile checking here.
        # Tell whether the 'filterAuth' gives extra permission to filter.
        # As this logic is an extension of the existing read-access check,
        # there is no dataset/table-level variant of the 'filterAuth'.
        if self.has_any_scope(field.filter_auth) and (field_auth := self.has_field_access(field)):
            # The if-statement checks filterAuth first. When it's defined, it likely denies access.
            # That way the other more complex checks are not needed.
            return (
                Permission(PermissionLevel.highest, source="field.filter_auth")
                if field.filter_auth
                else field_auth  # Return original permission if there is no filterAuth
            )
        else:
            return Permission.none

    def _has_dataset_auth_access(self, dataset: DatasetSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the dataset."""
        if self.has_any_scope(dataset.auth):
            return Permission(PermissionLevel.highest, source="dataset.auth")
        else:
            return Permission.none

    def _has_table_auth_access(self, table: DatasetTableSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the table."""
        if self.has_any_scope(table.auth) and self.has_any_scope(table.dataset.auth):
            return Permission(
                PermissionLevel.highest, source="table.auth" if table.auth else "dataset.auth"
            )
        else:
            return Permission.none

    def _has_field_auth_access(self, field: DatasetFieldSchema) -> Permission:
        """Tell whether the 'auth' rules give access to the table."""
        if (
            self.has_any_scope(field.auth)
            and self.has_any_scope(field.table.auth)
            and self.has_any_scope(field.table.dataset.auth)
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

    @cached_method()
    def _has_dataset_profile_access(self, dataset_id: str) -> Permission:
        """Give the permission access level for a dataset, as defined by the profile."""
        return max(
            (
                profile_dataset.permissions
                for profile_dataset in self.get_active_profile_datasets(dataset_id)
            ),
            default=Permission.none,
        )

    @cached_method()
    def _has_table_profile_access(self, table: DatasetTableSchema) -> Permission:
        """Give the permission level for a table.

        When a dataset defines global permissions without explicitly mentioning the table,
        these permissions are "inherited" and used.
        """
        dataset_id = table.dataset.id
        table_id = table.id
        max_permission = Permission.none

        for profile_dataset in self.get_active_profile_datasets(dataset_id):
            if max_permission.level == PermissionLevel.highest:
                break

            # If a profile defines "read" on the whole dataset, without explicitly
            # mentioning a table, this means the table can also be read unconditionally.
            profile_table = profile_dataset.tables.get(table_id, None)
            if profile_table is None:
                if dataset_permission := profile_dataset.permissions:
                    max_permission = max(max_permission, dataset_permission)

            # Otherwise see if the table can be included (mandatory filters match)
            elif self._may_include_profile_table(profile_table):
                max_permission = max(max_permission, profile_table.permissions)

        # Datasets may a permission that also covers this table,
        # but tables could also define an explicit permission. See who wins.
        return max_permission

    def _has_field_profile_access(self, field: DatasetFieldSchema) -> Permission:
        """Give the permission level for a field based on a profile.

        Fields have a special case: if a specific permission is defined, use that.
        This may "limit" the actual permission. For example, the table gives "read" permission,
        but the field may state "encoded" as the level. Since a default is defined for the field,
        that's being used.
        """
        field_id = field.id
        table_id = field.table.id
        max_permission = Permission.none

        # First see if there is an explicit definition for a field:
        for profile_dataset in self.get_active_profile_datasets(field.table.dataset.id):
            if max_permission.level == PermissionLevel.highest:
                break

            # If a profile defines "read" on the whole dataset, without explicitly
            # mentioning the table, this means the table can also be read unconditionally.
            profile_table = profile_dataset.tables.get(table_id, None)
            if profile_table is None:
                if dataset_permission := profile_dataset.permissions:
                    max_permission = max(max_permission, dataset_permission)
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
                if table_permission and table_permission.level > PermissionLevel.SUBOBJECTS_ONLY:
                    max_permission = max(max_permission, table_permission)
                continue

            max_permission = max(max_permission, field_permission)

        return max_permission

    @cached_method()
    def get_active_profile_datasets(self, dataset_id: str) -> list[ProfileDatasetSchema]:
        """Find all profiles that mention a dataset and match the scopes.

        This already checks whether the mandatory user scopes are set.
        """
        if self._all_profiles is None:
            self._all_profiles = []
        elif not isinstance(self._all_profiles, list):
            self._all_profiles = list(self._all_profiles)  # perform query on demand.

        return [
            profile_dataset
            for profile in self._all_profiles
            # Profiles are only activated when:
            # - ALL scopes are matched
            # - dataset is mentioned in the profile
            if self.has_all_scopes(profile.scopes)
            and (profile_dataset := profile.datasets.get(dataset_id)) is not None
        ]

    @cached_method()
    def get_active_profile_tables(
        self, dataset_id: str, table_id: str
    ) -> list[ProfileTableSchema]:
        """Find all profiles that mention a particular table and give access.

        This already checks whether the table passes the `mandatoryFilterSets` check,
        and whether the scopes of the dataset match.
        """
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
