from pg_grant import query
from pg_grant.sql import grant
from pg_grant import PgObjectType

def create_acl_from_profiles(engine, schema, profile_list):
    acl_list = query.get_all_table_acls(engine, schema='public')
    for profile in profile_list:
        scopes = profile["scopes"]
        for dataset, details in profile["schema_data"]["datasets"].items():
            for item in acl_list:
                if item.name.startswith(dataset+"_"):
                    priviliges = ["SELECT", ]
                    for grantee in scopes:
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema=schema)
                        engine.execute(grant_statement)

