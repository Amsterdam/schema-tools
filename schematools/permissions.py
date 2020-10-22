from pg_grant import query, parse_acl_item
from pg_grant.sql import grant, revoke
from pg_grant import PgObjectType
from .utils import to_snake_case
from .importer import get_table_name
from sqlalchemy.exc import SQLAlchemyError

PUBLIC_SCOPE = "OPENBAAR"


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


def apply_schema_and_profile_permissions(engine, ams_schema, profiles, role, scope, dry_run=False):
    if ams_schema:
        create_acl_from_schemas(engine, ams_schema, role, scope, dry_run)
    if profiles:
        profile_list = profiles.values()
        create_acl_from_profiles(engine, 'public', profile_list, role, scope)


def create_acl_from_profiles(engine, schema, profile_list, role, scope):
    acl_list = query.get_all_table_acls(engine, schema='public')
    priviliges = ["SELECT", ]
    grantee = role
    for profile in profile_list:
        if scope in profile["scopes"]:
            for dataset, details in profile["schema_data"]["datasets"].items():
                for item in acl_list:
                    if item.name.startswith(dataset+"_"):
                        grant_statement = grant(priviliges, PgObjectType.TABLE, item.name, grantee, grant_option=False, schema=schema)
                        print(grant_statement)
                        engine.execute(grant_statement)


def create_acl_from_schema(engine, ams_schema, role, scope, dry_run):
    grantee = role
    dataset_scope = ams_schema.auth if ams_schema.auth else {PUBLIC_SCOPE, }
    dataset_scope_set = {dataset_scope} if isinstance(dataset_scope, str) else set(dataset_scope)

    if dataset_scope_set - {PUBLIC_SCOPE}:
        print('Found dataset read permission for "{}" to scopes "{}"'.format(ams_schema.id, dataset_scope_set))
    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = "{}_{}".format(table.dataset.id, to_snake_case(table.id))  # een aantal table.id's zijn camelcase
        table_scope = table.auth if table.auth else dataset_scope
        table_scope_set = {table_scope} if isinstance(table_scope, str) else set(table_scope)
        if table.auth:
            print('Found table read permission for "{}" to scopes "{}"'.format(table_name, table_scope_set))
            print('"{}" overrules "{}" for read permission of "{}"'.format(table_scope_set, dataset_scope_set, table_name))
        contains_field_grants = False
        fields = [field for field in table.fields if '$ref' not in field]
        for field in fields:
            if field.auth:
                field_scope = field.auth
                field_scope_set = {field_scope} if isinstance(field_scope, str) else set(field_scope)
                print('Found field read permission for "{}" in table "{}" for scopes {}'.format(field.name, table_name, field_scope_set))
                contains_field_grants = True
                print('"{}" overrules "{}" for read permission of field {} in table {}"'.format(field_scope_set, table_scope_set, field.name, table_name))
                if scope in field_scope_set:
                    column_name = to_snake_case(field.name)
                    column_priviliges = ["SELECT ({})".format(column_name), ]  # the space after SELECT is very important
                    _execute_grant(engine, grant(column_priviliges, PgObjectType.TABLE, table_name, grantee, grant_option=False, schema='public'), dry_run=dry_run)
        if scope in table_scope_set:
            if contains_field_grants:
                #  only grant those fields which have no scope
                for field in fields:
                    if not field.auth:
                        column_name = to_snake_case(field.name)
                        column_priviliges = ["SELECT ({})".format(column_name), ]  # the space after SELECT is very important
                        _execute_grant(engine, grant(column_priviliges, PgObjectType.TABLE, table_name, grantee, grant_option=False, schema='public'), dry_run=dry_run)
            else:
                table_priviliges = ["SELECT", ]
                _execute_grant(engine, grant(table_priviliges, PgObjectType.TABLE, table_name, grantee, grant_option=False, schema='public'), dry_run=dry_run)


def create_acl_from_schemas(engine, schemas, role, scopes, dry_run):
    #  acl_list = query.get_all_table_acls(engine, schema='public')
    #  acl_table_list = [item.name for item in acl_list]
    #  table_names = list()
    for dataset_name, dataset_schema in schemas.items():
        create_acl_from_schema(engine, dataset_schema, role, scopes, dry_run)


def _execute_grant(engine, grant_statement, echo=True, dry_run=False):
    status_msg = "Skipped" if dry_run else "Executed"
    if echo:
        print(f"{status_msg} --> {grant_statement}")
    if not dry_run:
        try:
            engine.execute(grant_statement)
        except SQLAlchemyError as err:
            print(err)
