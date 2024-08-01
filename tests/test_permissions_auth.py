from __future__ import annotations

from schematools.permissions import Permission, UserScopes
from schematools.types import PermissionLevel


def _active_profiles(user_scopes: UserScopes, dataset_id: str, table_id: str) -> set[str]:
    """Tell which profiles are active for a table."""
    return {
        p.dataset.profile.name for p in user_scopes.get_active_profile_tables(dataset_id, table_id)
    }


def _active_profiles_by_dataset(user_scopes: UserScopes, dataset_id: str) -> set[str]:
    """Tell which profiles are active for a dataset."""
    return {p.profile.name for p in user_scopes.get_active_profile_datasets(dataset_id)}


class TestProfileActivation:
    def test_profile_scopes(self, profile_verkeer_medewerker_schema, profile_brk_encoded_schema):
        """Prove that profiles are only applied for the correct scopes."""
        user_scopes = UserScopes(
            query_params={},
            request_scopes=["FP/MD"],
            all_profiles=[profile_verkeer_medewerker_schema, profile_brk_encoded_schema],
        )

        assert _active_profiles_by_dataset(user_scopes, "brk") == set()
        assert _active_profiles_by_dataset(user_scopes, "verkeer") == {"verkeer_medewerker"}

    def test_mandatory_filter_match(self, brp_rname_profile_schema):
        """Prove that having a scope + mandatory filters actives a profile."""
        user_scopes = UserScopes(
            query_params={
                "postcode": "1234AB",
                "lastname": "foobar",
            },
            request_scopes=["BRP/RNAME"],
            all_profiles=[brp_rname_profile_schema],
        )

        assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == {"brp_medewerker"}

    def test_mandatory_filter_operators(self, brp_rname_profile_schema):
        """Prove that operators can't be used to circumvent list-filter limitations."""
        user_scopes = UserScopes(
            query_params={
                "postcode[like]": "*",
                "lastname[like]": "*",
            },
            request_scopes=["BRP/RNAME"],
            all_profiles=[brp_rname_profile_schema],
        )

        assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == set()

    def test_mandatory_filter_missing_scope(self, brp_rname_profile_schema):
        # Also prove the opposite: not getting access
        user_scopes = UserScopes(
            query_params={
                "postcode": "1234AB",
                "lastname": "foobar",
            },
            request_scopes=["BRP/R"],
            all_profiles=[brp_rname_profile_schema],
        )

        assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == set()

    def test_mandatory_filter_missing_query(self, brp_rname_profile_schema):
        # Also prove the opposite: not getting access
        user_scopes = UserScopes(
            query_params={
                "postcode": "1234AB",
            },
            request_scopes=["BRP/RNAME"],
            all_profiles=[brp_rname_profile_schema],
        )

        assert _active_profiles(user_scopes, "brp", "ingeschrevenpersonen") == set()


class TestTableAccess:

    def test_has_table_fields_access(self, id_auth_schema):
        """Prove that a table with one protected field cannot be accessed with OPENBAAR scope."""

        user_scopes = UserScopes(
            {},
            request_scopes=["OPENBAAR"],
        )
        table = id_auth_schema.get_table_by_id("base")
        assert not user_scopes.has_table_fields_access(table)

    def test_table_access_via_mandatory_filters(self, brp_schema, brp_rname_profile_schema):
        """Prove that table access can be granted if the query_params matches a profile."""
        table = brp_schema.get_table_by_id("ingeschrevenpersonen")

        user_scopes = UserScopes(
            query_params={
                "postcode": "1234AB",
            },
            request_scopes=["BRP/RNAME"],
            all_profiles=[brp_rname_profile_schema],
        )
        assert not user_scopes.has_table_access(table)

        # Create the object again, as it caches many results
        user_scopes = UserScopes(
            query_params={
                "postcode": "1234AB",
                "lastname": "foobar",
            },
            request_scopes=["BRP/RNAME"],
            all_profiles=[brp_rname_profile_schema],
        )
        assert user_scopes.has_table_access(table)


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

    def test_subfields_have_protection(self, subfield_auth_schema):
        """Prove that the subfields of a protected field are also protected."""

        user_scopes = UserScopes(
            {},
            request_scopes=["OPENBAAR"],
        )
        table = subfield_auth_schema.get_table_by_id("base")
        subfield = table.get_field_by_id("soortCultuurOnbebouwd").subfields[0]
        assert not user_scopes.has_field_access(subfield)
