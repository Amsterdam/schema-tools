from schematools.importer.ndjson import NDJSONImporter
from pg_grant import query
from schematools.permissions import create_acl_from_profiles
from sqlalchemy.exc import ProgrammingError


def test_permissions_setting(here, engine, gebieden_schema, dbsession):
    test_profile = {
        "name": "gebieden_test",
        "scopes": ["FP/MD", ],
        "schema_data": {
            "datasets": {
                "gebieden": {
                    "permissions": "read"
                }
            }
        }
    }
    ndjson_path = here / "files" / "data" / "gebieden.ndjson"
    importer = NDJSONImporter(gebieden_schema, engine)
    importer.generate_tables("bouwblokken", truncate=True)
    importer.load_file(ndjson_path)

    #  create required Roles
    for scope in test_profile["scopes"]:
        try:
            engine.execute('CREATE ROLE "{}"'.format(scope))
        except ProgrammingError:
            #  psycopg2.errors.DuplicateObject
            pass

    #  check if ACL is initially None
    acl_data = query.get_all_table_acls(engine, schema='public')
    tablenames = set([str(entry.name) for entry in acl_data])
    assert "gebieden_bouwblokken" in tablenames
    for entry in acl_data:
        if entry.name.startswith("gebieden_"):
            assert entry.acl is None

    # transform profiles into database level ACL
    profile_list = [test_profile]
    create_acl_from_profiles(engine, schema='public', profile_list=profile_list)

    # check if new ACL is set correctly
    new_acl_data = query.get_all_table_acls(engine, schema='public')
    assert len(new_acl_data) > 0
    for entry in new_acl_data:
        if entry.name.startswith("gebieden_"):
            print(entry.acl)
            assert '"FP/MD"=r/dataservices' in entry.acl  # role FP/MD has read permission, given by dataservices
            assert 'dataservices=arwdDxt/dataservices' in entry.acl  # the default is now explicitly set
            assert len(entry.acl) == 2  # and there is nothing more



