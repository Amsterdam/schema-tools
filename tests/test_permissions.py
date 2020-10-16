import json
from schematools.importer.ndjson import NDJSONImporter
from pg_grant import query
from schematools.permissions import create_acl_from_profiles, apply_schema_and_profile_permissions
from sqlalchemy.exc import ProgrammingError
import pytest
from schematools.types import DatasetSchema


def test_permissions_apply(here, engine, gebieden_schema, dbsession):
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)
    acl_data = query.get_all_table_acls(engine, schema='public')
    tablenames = set([str(entry.name) for entry in acl_data])
    assert "gebieden_bouwblokken" in tablenames
    for entry in acl_data:
        if entry.name.startswith("gebieden_"):
            assert entry.acl is None
    ams_schema = {gebieden_schema.id: gebieden_schema}
    profile_path = here / "files" / "profiles" / "gebieden_test.json"
    with open(profile_path) as f:
        profile = json.load(f)
    profiles = {profile["name"]: profile}

    create_role(engine, "level_a")
    create_role(engine, "level_b")
    create_role(engine, "level_c")

    apply_schema_and_profile_permissions(engine, ams_schema, profiles, "level_a", "LEVEL/A")
    apply_schema_and_profile_permissions(engine, ams_schema, profiles, "level_b", "LEVEL/B")
    apply_schema_and_profile_permissions(engine, ams_schema, profiles, "level_c", "LEVEL/C")

    check_table_permission_denied(engine, "level_a", "gebieden_bouwblokken")
    check_table_permission_granted(engine, "level_b", "gebieden_bouwblokken")
    check_table_permission_denied(engine, "level_c", "gebieden_bouwblokken")


def create_role(engine, role):
    #  If role already exists just fail and ignore. This may happen if a previous pytest did not terminate correctly.
    try:
        engine.execute('CREATE ROLE "{}"'.format(role))
    except ProgrammingError:
        #  psycopg2.errors.DuplicateObject
        pass
    return role


def check_table_permission_denied(engine, role, table):
    """Check if role has no SELECT permission on table. Fail if role or table does not exist."""
    with pytest.raises(Exception) as e_info:
        with engine.begin() as connection:
            connection.execute("SET ROLE {}".format(role))
            connection.execute("SELECT * FROM {}".format(table))
            connection.execute("RESET ROLE")
    assert "permission denied for table gebieden_bouwblokken" in str(e_info)


def check_table_permission_granted(engine, role, table):
    "Check if role has SELECT permission on table. Fail if role or table does not exist."
    with engine.begin() as connection:
        connection.execute("SET ROLE {}".format(role))
        result = connection.execute("SELECT * FROM {}".format(table))
        connection.execute("RESET ROLE")
    rows = [row for row in result]
    assert len(rows) > 0
