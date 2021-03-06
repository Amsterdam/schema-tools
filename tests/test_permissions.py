import json

import pytest
from psycopg2.errors import DuplicateObject
from sqlalchemy.exc import ProgrammingError

from schematools.importer.ndjson import NDJSONImporter
from schematools.permissions import apply_schema_and_profile_permissions


def test_auto_permissions(here, engine, gebieden_schema_auth, dbsession):
    """
    Prove that roles are automatically created for each scope in the schema
    LEVEL/A --> scope_level_a
    LEVEL/B --> scope_level_b
    LEVEL/C --> scope_level_c
    """
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema_auth, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

    # Setup schema and profile
    ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    # Apply the permissions from Schema and Profiles.
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "AUTO", "ALL", create_roles=True
    )
    _check_permission_granted(engine, "scope_level_a", "gebieden_buurten")
    _check_permission_granted(
        engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
    )
    _check_permission_granted(engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid")


def test_openbaar_permissions(here, engine, afval_schema, dbsession):
    """
    Prove that the default auth scope is "OPENBAAR".
    """

    importer = NDJSONImporter(afval_schema, engine)
    importer.generate_db_objects("containers", truncate=True, ind_extra_index=False)
    importer.generate_db_objects("clusters", truncate=True, ind_extra_index=False)

    # Setup schema and profile
    ams_schema = {afval_schema.id: afval_schema}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    # Create postgres roles
    _create_role(engine, "openbaar")
    _create_role(engine, "bag_r")
    # Check if the roles exist, the tables exist,
    # and the roles have no read privilige on the tables.
    _check_permission_denied(engine, "openbaar", "afvalwegingen_containers")
    _check_permission_denied(engine, "bag_r", "afvalwegingen_clusters")

    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "openbaar", "OPENBAAR"
    )
    apply_schema_and_profile_permissions(engine, "public", ams_schema, profiles, "bag_r", "BAG/R")

    _check_permission_granted(engine, "openbaar", "afvalwegingen_containers")
    _check_permission_denied(engine, "openbaar", "afvalwegingen_clusters")
    _check_permission_denied(engine, "bag_r", "afvalwegingen_containers")
    _check_permission_granted(engine, "bag_r", "afvalwegingen_clusters")


def test_interacting_permissions(here, engine, gebieden_schema_auth, dbsession):
    """
    Prove that dataset, table, and field permissions are set
    according to the "OF-OF" Exclusief principle:

    * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
      behalve tabel bouwblokken.
    * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
      behalve beginGeldigheid.
    * Een user met scope LEVEL/C mag veld beginGeldigheid zien.
    """

    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema_auth, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

    # Setup schema and profile
    ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    # Create postgres roles
    test_roles = ["level_a", "level_b", "level_c"]
    for test_role in test_roles:
        _create_role(engine, test_role)

    # Check if the roles exist, the tables exist,
    # and the roles have no read privilige on the tables.
    for test_role in test_roles:
        for table in ["gebieden_bouwblokken", "gebieden_buurten"]:
            _check_permission_denied(engine, test_role, table)

    # Apply the permissions from Schema and Profiles.
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_a", "LEVEL/A"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_b", "LEVEL/B"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_c", "LEVEL/C"
    )

    # Check if the read priviliges are correct
    _check_permission_denied(engine, "level_a", "gebieden_bouwblokken")
    _check_permission_granted(engine, "level_a", "gebieden_buurten")

    _check_permission_granted(engine, "level_b", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_denied(engine, "level_b", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_b", "gebieden_buurten")

    _check_permission_denied(engine, "level_c", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_granted(engine, "level_c", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_c", "gebieden_buurten")


def test_auth_list_permissions(here, engine, gebieden_schema_auth_list, dbsession):
    """
    Prove that dataset, table, and field permissions are set,
    according to the "OF-OF" Exclusief principle.
    Prove that when the auth property is a list of scopes, this is interpreted as "OF-OF".

    * Een user met scope LEVEL/A1 of LEVEL/A2 mag alles uit de dataset gebieden zien,
      behalve tabel bouwblokken.
    * Een user met scope LEVEL/B1 of LEVEL/B2 mag alle velden van tabel bouwblokken zien,
      behalve beginGeldigheid.
    * Een user met scope LEVEL/C1 of LEVEL/B2 mag veld beginGeldigheid zien.
    """

    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema_auth_list, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

    # Setup schema and profile
    ams_schema = {gebieden_schema_auth_list.id: gebieden_schema_auth_list}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    # Create postgres roles
    test_roles = [
        "level_a1",
        "level_a2",
        "level_b1",
        "level_b2",
        "level_c1",
        "level_c2",
    ]
    for test_role in test_roles:
        _create_role(engine, test_role)

    # Check if the roles exist, the tables exist,
    # and the roles have no read privilige on the tables.
    for test_role in test_roles:
        for table in ["gebieden_bouwblokken", "gebieden_buurten"]:
            _check_permission_denied(engine, test_role, table)

    # Apply the permissions from Schema and Profiles.
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_a1", "LEVEL/A1"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_b1", "LEVEL/B1"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_c1", "LEVEL/C1"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_a2", "LEVEL/A2"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_b2", "LEVEL/B2"
    )
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "level_c2", "LEVEL/C2"
    )

    # Check if the read priviliges are correct
    _check_permission_denied(engine, "level_a1", "gebieden_bouwblokken")
    _check_permission_granted(engine, "level_a1", "gebieden_buurten")
    _check_permission_denied(engine, "level_a2", "gebieden_bouwblokken")
    _check_permission_granted(engine, "level_a2", "gebieden_buurten")

    _check_permission_granted(engine, "level_b1", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_denied(engine, "level_b1", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_b1", "gebieden_buurten")
    _check_permission_granted(engine, "level_b2", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_denied(engine, "level_b2", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_b2", "gebieden_buurten")

    _check_permission_denied(engine, "level_c1", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_granted(engine, "level_c1", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_c1", "gebieden_buurten")

    _check_permission_denied(engine, "level_c2", "gebieden_bouwblokken", "id, eind_geldigheid")
    _check_permission_granted(engine, "level_c2", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "level_c2", "gebieden_buurten")


def test_auto_create_roles(here, engine, gebieden_schema_auth, dbsession):
    """
    Prove that dataset, table, and field permissions are set according,
    to the "OF-OF" Exclusief principle:

    * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
      behalve tabel bouwblokken.
    * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
      behalve beginGeldigheid.
    * Een user met scope LEVEL/C mag veld beginGeldigheid zien.

    Drie corresponderende users worden automatisch aangemaakt:
    'scope_level_a', 'scope_level_b', en 'scope_level_c;
    """

    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema_auth, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

    # Setup schema and profile
    ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    # These tests commented out due to: Error when trying to teardown test databases
    # Roles may still exist from previous test run. Uncomment when fixed:
    # _check_role_does_not_exist(engine, "scope_level_a")
    # _check_role_does_not_exist(engine, "scope_level_b")
    # _check_role_does_not_exist(engine, "scope_level_c")

    # Apply the permissions from Schema and Profiles.
    apply_schema_and_profile_permissions(
        engine, "public", ams_schema, profiles, "AUTO", "ALL", create_roles=True
    )
    # Check if roles exist and the read priviliges are correct
    _check_permission_denied(engine, "scope_level_a", "gebieden_bouwblokken")
    _check_permission_granted(engine, "scope_level_a", "gebieden_buurten")

    _check_permission_granted(
        engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
    )
    _check_permission_denied(engine, "scope_level_b", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "scope_level_b", "gebieden_buurten")

    _check_permission_denied(
        engine, "scope_level_c", "gebieden_bouwblokken", "id, eind_geldigheid"
    )
    _check_permission_granted(engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid")
    _check_permission_denied(engine, "scope_level_c", "gebieden_buurten")


def test_single_dataset_permissions(
    here, engine, gebieden_schema_auth, meetbouten_schema, dbsession
):
    """
    Prove when revoking grants on one dataset, other datasets are unaffected.
    """

    # dataset 1: gebieden
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema_auth, engine)
    importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
    importer.load_file(ndjson_path)
    importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

    # dataset 2: meetbouten
    ndjson_path = here / "files" / "data" / "meetbouten.ndjson"
    importer = NDJSONImporter(meetbouten_schema, engine)
    importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
    importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
    importer.generate_db_objects("referentiepunten", truncate=True, ind_extra_index=False)

    # Apply the permissions to gebieden
    apply_schema_and_profile_permissions(
        engine, "public", gebieden_schema_auth, None, "AUTO", "ALL", create_roles=True
    )
    # Check perms on gebieden
    _check_permission_granted(engine, "scope_level_a", "gebieden_buurten")
    _check_permission_granted(
        engine, "scope_level_b", "gebieden_bouwblokken", "id, eind_geldigheid"
    )
    _check_permission_granted(engine, "scope_level_c", "gebieden_bouwblokken", "begin_geldigheid")

    # Apply the permissions to meetbouten
    apply_schema_and_profile_permissions(
        engine, "public", meetbouten_schema, None, "AUTO", "ALL", create_roles=True
    )
    # Check perms on meetbouten
    _check_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten")

    # Revoke permissions for dataset gebieden and set grant again
    apply_schema_and_profile_permissions(
        engine,
        pg_schema="public",
        ams_schema=gebieden_schema_auth,
        profiles=None,
        role="AUTO",
        scope="ALL",
        create_roles=True,
        revoke=True,
    )
    # Check perms again on meetbouten
    _check_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten")


def _create_role(engine, role):
    # If role already exists just fail and ignore.
    # This may happen if a previous pytest did not terminate correctly.
    try:
        engine.execute('CREATE ROLE "{}"'.format(role))
    except ProgrammingError as e:
        if not isinstance(e.orig, DuplicateObject):
            raise


def _check_role_does_not_exist(engine, role):
    """Check if role does not exist"""
    with engine.begin() as connection:
        result = connection.execute(f"SELECT rolname FROM pg_roles WHERE rolname='{role}'")
        rows = [row for row in result]
        assert len(rows) == 0


def _check_permission_denied(engine, role, table, column="*"):
    """Check if role has no SELECT permission on table.
    Fail if role, table or column does not exist."""
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute("SELECT {} FROM {}".format(column, table))
            connection.execute("RESET ROLE")
    assert "permission denied for table {}".format(table) in str(e_info)


def _check_permission_granted(engine, role, table, column="*"):
    """Check if role has SELECT permission on table.
    Fail if role, table or column does not exist."""
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute("SELECT {} FROM {}".format(column, table))
        connection.execute("RESET ROLE")
    assert result
