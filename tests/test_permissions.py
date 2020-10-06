from schematools.importer.ndjson import NDJSONImporter
from pg_grant import query
from schematools.permissions import create_acl_from_profiles
from sqlalchemy.exc import ProgrammingError
def test_permissions_setting(here, engine, parkeervakken_schema, dbsession):
    test_profile = {
        "name": "test",
        "scopes": ["FP/MD", ],
        "schema_data": {
            "datasets": {
                "parkeervakken": {
                    "permissions": "read"
                },
                "gebieden": {
                    "tables": {
                        "bouwblokken": {
                            "fields": {
                                "ligtInBuurt": "encoded"
                            }
                        }
                    }
                }
            }
        }
    }

    ndjson_path = here / "files" / "data" / "parkeervakken.ndjson"
    importer = NDJSONImporter(parkeervakken_schema, engine)
    importer.generate_tables("parkeervakken", truncate=True)
    importer.load_file(ndjson_path)
    acl_data = query.get_all_table_acls(engine, schema='public')
    #  check if ACL is initially None
    for entry in acl_data:
        if entry.name.startswith("parkeervakken_"):
            assert entry.acl is None

    #  create required Roles
    for scope in test_profile["scopes"]:
        try:
            engine.execute('CREATE ROLE "{}"'.format(scope))
        except ProgrammingError:
            #  psycopg2.errors.DuplicateObject
            pass
    profile_list = [test_profile]
    grant_statements = create_acl_from_profiles(engine, schema='public', profile_list=profile_list)
    # execute GRANT statements
    for grant_statement in grant_statements:
        engine.execute(grant_statement)
    new_acl_data = query.get_all_table_acls(engine, schema='public')
    assert len(new_acl_data) > 0
    tablenames = set([str(entry.name) for entry in new_acl_data])
    for t in tablenames:
        if t.startswith("gebieden"):
            assert False, t
    #  BUG hier gaat het niet goed. Moeite om parkeervakken tabellen terug te vinden
    for entry in new_acl_data:
        if entry.name.startswith("parkeervakken"):
            assert entry.acl is None
