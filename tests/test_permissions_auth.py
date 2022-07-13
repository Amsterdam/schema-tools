from __future__ import annotations

from schematools.permissions import Permission, UserScopes
from schematools.types import PermissionLevel


def _active_profiles(
    user_scopes: UserScopes, dataset_id: str, table_id: str | None = None
) -> set[str]:
    """Shorthand to get active profile names"""
    if table_id is not None:
        return {
            p.dataset.profile.name
            for p in user_scopes.get_active_profile_tables(dataset_id, table_id)
        }
    else:
        return {p.profile.name for p in user_scopes.get_active_profile_datasets(dataset_id)}


def test_profile_scopes(profile_verkeer_medewerker_schema, profile_brk_encoded_schema):
    """Prove that profiles are only applied for the correct scopes."""
    user_scopes = UserScopes(
        query_params={},
        request_scopes=["FP/MD"],
        all_profiles=[profile_verkeer_medewerker_schema, profile_brk_encoded_schema],
    )

    assert _active_profiles(user_scopes, "brk") == set()
    assert _active_profiles(user_scopes, "verkeer") == {"verkeer_medewerker"}


def test_mandatory_filters(brp_r_profile_schema):
    """Prove that having a scope + mandatory filters actives a profile."""
    user_scopes = UserScopes(
        query_params={
            "postcode": "1234AB",
            "lastname": "foobar",
        },
        request_scopes=["BRP/R"],
        all_profiles=[brp_r_profile_schema],
    )

    assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == {"brp_medewerker"}

    # Also prove the opposite: not getting access
    user_scopes = UserScopes(
        query_params={
            "postcode": "1234AB",
        },
        request_scopes=["BRP/R"],
        all_profiles=[brp_r_profile_schema],
    )

    assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == set()


class TestFieldAccess:
    """All variations to test the field access level."""

    def test_auth_access(self, brk_schema, profile_brk_encoded_schema, verblijfsobjecten_schema):
        """Check that user who is authorized for all authorization checks can access data"""
        user_scopes = UserScopes(
            {},
            request_scopes=["BRK/RSN"],
            all_profiles=[profile_brk_encoded_schema],
        )
        table = brk_schema.get_table_by_id("kadastraleobjecten")

        auth_result = user_scopes.has_field_access(table.get_field_by_id("id"))
        assert auth_result == Permission(PermissionLevel.READ)

    def test_profile_combines(
        self,
        kadastraleobjecten_schema,
        profile_brk_encoded_schema,
        profile_brk_read_id_schema,
        profile_verkeer_medewerker_schema,
    ):
        """Checks that profiles combine for a given user.
        And that he can access data accordingly, while regular medewerker not."""
        # Both Profiles used
        user_scopes = UserScopes(
            {},
            request_scopes=["BRK/RID", "BRK/ENCODED"],
            all_profiles=[
                profile_brk_encoded_schema,
                profile_brk_read_id_schema,
                profile_verkeer_medewerker_schema,  # should be ignored
            ],
        )
        kadastraleobjecten_schema["auth"] = ["MAG/NIET"]
        table = kadastraleobjecten_schema.get_table_by_id("kadastraleobjecten")

        can_read = Permission(PermissionLevel.READ)
        as_encoded = Permission(PermissionLevel.ENCODED)

        assert user_scopes.has_field_access(table.get_field_by_id("id")) == can_read
        assert user_scopes.has_field_access(table.get_field_by_id("volgnummer")) == can_read
        assert user_scopes.has_field_access(table.get_field_by_id("identificatie")) == as_encoded
        assert not user_scopes.has_field_access(table.get_field_by_id("registratiedatum"))

    def test_has_perm_dataset_field_read_profile(
        self, brk_schema, profile_brk_read_id_schema, verblijfsobjecten_schema
    ):
        """Check that user with one profile can access data, while regular medeweker not."""
        # Read Profile used
        user_scopes = UserScopes(
            {},
            request_scopes=["BRK/RID"],
            all_profiles=[profile_brk_read_id_schema],
        )
        table = brk_schema.get_table_by_id("kadastraleobjecten")

        assert user_scopes.has_field_access(table.get_field_by_id("id"))
        assert user_scopes.has_field_access(table.get_field_by_id("volgnummer"))
        assert not user_scopes.has_field_access(table.get_field_by_id("identificatie"))

    def test_profile_field_inheritance_two_profiles(
        self, kadastraleobjecten_schema, profile_brk_encoded_schema, profile_brk_read_id_schema
    ):
        """Tests that profiles field permissions inherit properly"""

        user_scopes = UserScopes(
            {},
            request_scopes=["BRK/ENCODED", "BRK/RID"],
            all_profiles=[profile_brk_encoded_schema, profile_brk_read_id_schema],
        )
        kadastraleobjecten_schema["auth"] = ["MAG/NIET"]  # monkeypatch schema
        table = kadastraleobjecten_schema.get_table_by_id("kadastraleobjecten")

        can_read = Permission(PermissionLevel.READ)
        as_encoded = Permission(PermissionLevel.ENCODED)

        assert user_scopes.has_field_access(table.get_field_by_id("id")) == can_read
        assert user_scopes.has_field_access(table.get_field_by_id("volgnummer")) == can_read
        assert user_scopes.has_field_access(table.get_field_by_id("identificatie")) == as_encoded
        assert not user_scopes.has_field_access(table.get_field_by_id("registratiedatum"))

    def test_profile_field_inheritance_from_dataset(
        self, kadastraleobjecten_schema, profile_brk_encoded_schema, profile_brk_read_id_schema
    ):
        """Tests dataset permissions overrule lower profile permissions"""
        user_scopes = UserScopes(
            {},
            request_scopes=["BRK/ENCODED", "DATASET/SCOPE"],
            all_profiles=[profile_brk_encoded_schema, profile_brk_read_id_schema],
        )

        kadastraleobjecten_schema["auth"] = ["DATASET/SCOPE"]  # monkeypatch schema
        table = kadastraleobjecten_schema.get_table_by_id("kadastraleobjecten")

        expect = Permission(PermissionLevel.READ)
        assert user_scopes.has_field_access(table.get_field_by_id("id")) == expect
        assert user_scopes.has_field_access(table.get_field_by_id("volgnummer")) == expect
        assert user_scopes.has_field_access(table.get_field_by_id("identificatie")) == expect
        assert user_scopes.has_field_access(table.get_field_by_id("registratiedatum")) == expect
