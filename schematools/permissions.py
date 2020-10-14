from pg_grant import query, parse_acl_item
from pg_grant.sql import grant, revoke
from pg_grant import PgObjectType
from .utils import to_snake_case
from .importer import get_table_name
from sqlalchemy.exc import SQLAlchemyError


def introspect_permissions(engine, role):
    schema_relation_infolist = query.get_all_table_acls(engine, schema='public')
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print('role "{}" has priviliges {} on table "{}"'.format(role, ','.join(acl.privs), schema_relation_info.name))


def revoke_permissions(engine, role):
    grantee =role
    schema_relation_infolist = query.get_all_table_acls(engine, schema='public')
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print('revoking ALL priviliges of role "{}" on table "{}"'.format(role, schema_relation_info.name))
                    revoke_statement = revoke("ALL", PgObjectType.TABLE, schema_relation_info.name, grantee)
                    engine.execute(revoke_statement)


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


def create_acl_from_schema(engine, ams_schema, role, permitted_scopes):
    priviliges = ["SELECT", ]
    grantee = role
    dataset_scope = ams_schema.auth if ams_schema.auth else 'PUBLIC'
    if dataset_scope != 'PUBLIC':
        print('Found dataset read permission for "{}" to scope "{}"'.format(ams_schema.id, dataset_scope))
    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = "{}_{}".format(table.dataset.id, to_snake_case(table.id))  # een aantal table.id's zijn camelcase
        table_scope = table.auth if table.auth else dataset_scope
        if table.auth:
            print('Found table read permission for "{}" to scope "{}"'.format(table_name, table_scope))
        if dataset_scope != 'PUBLIC' and dataset_scope != table_scope:
            print('"{}" overrules "{}" for read permission of "{}"'.format(table_scope, dataset_scope, table_name))
        if table_scope and table_scope in permitted_scopes.split(","):
            grant_statement = grant(priviliges, PgObjectType.TABLE, table_name, grantee, grant_option=False, schema='public')
            print("--> {}".format(grant_statement))
            try:
                engine.execute(grant_statement)
            except SQLAlchemyError as err:
                print(err)

def create_acl_from_schemas(engine, schemas, role, scopes):
    #  acl_list = query.get_all_table_acls(engine, schema='public')
    #  acl_table_list = [item.name for item in acl_list]
    #  table_names = list()
    for dataset_name, dataset_schema in schemas.items():
        create_acl_from_schema(engine, dataset_schema, role, scopes)
