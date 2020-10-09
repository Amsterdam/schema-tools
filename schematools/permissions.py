from pg_grant import query
from pg_grant.sql import grant
from pg_grant import PgObjectType


def create_acl_from_profiles(engine, schema, profile_list, role, scopes):
    acl_list = query.get_all_table_acls(engine, schema='public')
    priviliges = ["SELECT", ]
    grantee = role
    for profile in profile_list:
        if set(scopes.split(",")).intersection(set(profile["scopes"])):
            for dataset, details in profile["schema_data"]["datasets"].items():
                for item in acl_list:
                    if item.name.startswith(dataset+"_"):
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema=schema)
                        print(grant_statement)
                        engine.execute(grant_statement)


def create_acl_from_schema(engine, ams_schema, role, scopes):
    #  In progress: Only Dataset Level Authorization
    acl_list = query.get_all_table_acls(engine, schema='public')
    #  If schema.auth matches a scope, role will get read permission on associated tables
    priviliges = ["SELECT", ]
    grantee = role
    if ams_schema.auth:
        for scope in scopes.split(","):
            if ams_schema.auth == scope:
                for item in acl_list:
                    if item.name.startswith(ams_schema.id + "_"):
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema='public')
                        print(grant_statement)
                        engine.execute(grant_statement)


