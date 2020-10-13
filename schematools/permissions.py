from pg_grant import query
from pg_grant.sql import grant
from pg_grant import PgObjectType
from .utils import to_snake_case
from .importer import get_table_name


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
    analyze_schema(ams_schema)
    if ams_schema.auth:
        for scope in scopes.split(","):
            if ams_schema.auth == scope:
                for item in acl_list:
                    if item.name.startswith(ams_schema.id + "_"):
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema='public')
                        print(grant_statement)
                        engine.execute(grant_statement)


def create_acl_from_schemas(engine, schemas, role, scopes):
    print("hallo")
    acl_list = query.get_all_table_acls(engine, schema='public')
    acl_table_list = [item.name for item in acl_list]
    table_names = list()
    for dataset_name, dataset_schema in schemas.items():
        table_names = table_names + analyze_schema(dataset_schema)
        #print(dataset_name)
        if dataset_schema.auth:
            print("auth {}".format(dataset_schema.auth))
        if dataset_name=="gebieden":
            analyze_schema(dataset_schema)
    print(len(table_names), len(acl_table_list))
    print("tables wel in postgres, niet uit schema gehaald")
    print(set(acl_table_list) - set(table_names))
    print("tables uit schema die niet in postgres zitten")
    print(set(table_names) - set(acl_table_list))

def analyze_schema(dataset_schema):
    #print("analyze_schema {}".format(dataset_schema.id))
    table_names = list()
    for table in dataset_schema.get_tables(include_nested=True, include_through=True):
        if True: #table.auth:
            table_name = "{}_{}".format(table.dataset.id, to_snake_case(table.id))  # een aantal table.id's zijn camelcase
            table_names.append(table_name)
        if any(x.isupper() for x in table_name):  # gaat iets fout met naamgeving, mag geen capitals
            print("ALERT: {} should be {}".format(table_name, to_snake_case(table_name)))
            print(table.identifier)
    return table_names
