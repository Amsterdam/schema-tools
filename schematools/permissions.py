from pg_grant import query
from pg_grant.sql import grant
from pg_grant import PgObjectType

def create_acl_from_profiles(engine, schema, profile_list):
    # need list of all tablenames

    acl_list = query.get_all_table_acls(engine, schema='public')
    grant_statements = []
    for profile in profile_list:
        scopes = profile["scopes"]
        for dataset, details in profile["schema_data"]["datasets"].items():
            for item in acl_list:
                #  print(item)
                #  print(item.name)
                if item.name.startswith(dataset+"_"):
                    #  deze tabel hoort bij de dataset.
                    #  pg_grant.sql.grant(privileges, type: pg_grant.types.PgObjectType, target, grantee, grant_option = False, schema = None, arg_types = None, quote_subname = True)
                    priviliges = ["SELECT", ]
                    for grantee in scopes:
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema=schema)
                        grant_statements.append(str(grant_statement))
    return grant_statements



