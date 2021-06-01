"""Create GRANT statements to give roles very specific access to the database."""
from pg_grant import PgObjectType, parse_acl_item, query
from pg_grant.sql import grant, revoke
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from schematools.types import DatasetSchema
from schematools.utils import to_snake_case

PUBLIC_SCOPE = "OPENBAAR"

existing_roles = set()


def introspect_permissions(engine, role):
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print(
                        'role "{}" has priviliges {} on table "{}"'.format(
                            role, ",".join(acl.privs), schema_relation_info.name
                        )
                    )


def revoke_permissions(engine, role):
    grantee = role
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    print(
                        'revoking ALL priviliges of role "{}" on table "{}"'.format(
                            role, schema_relation_info.name
                        )
                    )
                    revoke_statement = revoke(
                        "ALL", PgObjectType.TABLE, schema_relation_info.name, grantee
                    )
                    engine.execute(revoke_statement)


def apply_schema_and_profile_permissions(
    engine,
    pg_schema,
    ams_schema,
    profiles,
    role,
    scope,
    set_read_permissions=True,
    set_write_permissions=True,
    dry_run=False,
    create_roles=False,
    revoke=False,
):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        if ams_schema:
            create_acl_from_schemas(
                session,
                pg_schema,
                ams_schema,
                role,
                scope,
                set_read_permissions,
                set_write_permissions,
                dry_run,
                create_roles,
                revoke,
            )
        if profiles:
            profile_list = profiles.values()
            create_acl_from_profiles(engine, pg_schema, profile_list, role, scope)
        session.commit()
    except Exception:
        session.rollback()
        print("warning: session rolled back")
        raise
    finally:
        session.close()


def create_acl_from_profiles(engine, pg_schema, profile_list, role, scope):
    # NOTE: Rudimentary, not ready for production.
    acl_list = query.get_all_table_acls(engine, schema=pg_schema)
    priviliges = [
        "SELECT",
    ]
    grantee = role
    for profile in profile_list:
        if scope in profile["scopes"]:
            for dataset, _details in profile["schema_data"]["datasets"].items():
                for item in acl_list:
                    if item.name.startswith(dataset + "_"):
                        grant_statement = grant(
                            priviliges,
                            PgObjectType.TABLE,
                            item.name,
                            grantee,
                            grant_option=False,
                            schema=pg_schema,
                        )
                        print(grant_statement)
                        engine.execute(grant_statement)


def set_dataset_write_permissions(session, pg_schema, ams_schema, dry_run, create_roles):
    grantee = f"write_{ams_schema.id}"
    if create_roles:
        _create_role_if_not_exists(session, grantee, dry_run=dry_run)
    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = "{}_{}".format(
            table.dataset.id, to_snake_case(table.id)
        )  # een aantal table.id's zijn camelcase
        table_privileges = ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES"]
        _execute_grant(
            session,
            grant(
                table_privileges,
                PgObjectType.TABLE,
                table_name,
                grantee,
                grant_option=False,
                schema=pg_schema,
            ),
            dry_run=dry_run,
        )


def set_dataset_read_permissions(
    session, pg_schema, ams_schema, role, scope, dry_run, create_roles
):
    grantee = None if role == "AUTO" else role
    if create_roles and grantee:
        _create_role_if_not_exists(session, grantee)
    dataset_scope = (
        ams_schema.auth
        if ams_schema.auth
        else {
            PUBLIC_SCOPE,
        }
    )
    dataset_scope_set = {dataset_scope} if isinstance(dataset_scope, str) else set(dataset_scope)
    if dataset_scope_set - {PUBLIC_SCOPE}:
        print(
            'Found dataset read permission for "{}" to scopes "{}"'.format(
                ams_schema.id, dataset_scope_set
            )
        )

    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = "{}_{}".format(
            table.dataset.id, to_snake_case(table.id)
        )  # een aantal table.id's zijn camelcase
        table_scope = table.auth if table.auth else dataset_scope
        table_scope_set = {table_scope} if isinstance(table_scope, str) else set(table_scope)
        if table.auth:
            print(f'Found table read permission for "{table_name}" to scopes "{table_scope_set}"')
            print(
                f'"{table_scope_set}" overrules "{dataset_scope_set}"'
                f' for read permission of "{table_name}"'
            )
        contains_field_grants = False
        fields = [field for field in table.fields if field.name != "schema"]
        for field in fields:
            if field.auth:
                field_scope = field.auth
                field_scope_set = (
                    {field_scope} if isinstance(field_scope, str) else set(field_scope)
                )
                print(
                    f'Found field read permission for "{field.name}" in'
                    f' table "{table_name}" for scopes {field_scope_set}'
                )
                contains_field_grants = True
                print(
                    f'"{field_scope_set}" overrules "{table_scope_set}" for read'
                    f' permission of field {field.name} in table {table_name}"'
                )
                if role == "AUTO":
                    grantees = [scope_to_role(scope) for scope in field_scope_set]
                elif scope in field_scope_set:
                    grantees = [role]
                else:
                    grantees = []
                for grantee in grantees:
                    if create_roles:
                        _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                    column_name = to_snake_case(field.name)
                    # the space after SELECT is very important
                    column_priviliges = [f"SELECT ({column_name})"]
                    _execute_grant(
                        session,
                        grant(
                            column_priviliges,
                            PgObjectType.TABLE,
                            table_name,
                            grantee,
                            grant_option=False,
                            schema=pg_schema,
                        ),
                        dry_run=dry_run,
                    )
        if role == "AUTO":
            grantees = [scope_to_role(scope) for scope in table_scope_set]
        elif scope in table_scope_set:
            grantees = [role]
        else:
            grantees = []

        if contains_field_grants:
            # Only grant those fields without their own scope.
            # The other field have already been granted above
            for grantee in grantees:
                if create_roles:
                    _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                for field in fields:
                    if not field.auth:
                        column_name = to_snake_case(field.name)
                        # the space after SELECT is very important:
                        column_priviliges = ["SELECT ({})".format(column_name)]
                        _execute_grant(
                            session,
                            grant(
                                column_priviliges,
                                PgObjectType.TABLE,
                                table_name,
                                grantee,
                                grant_option=False,
                                schema=pg_schema,
                            ),
                            dry_run=dry_run,
                        )
        else:
            # we can grant the whole table instead of field by field
            for grantee in grantees:
                if create_roles:
                    _create_role_if_not_exists(session, grantee, dry_run=dry_run)
                table_privileges = ["SELECT"]
                _execute_grant(
                    session,
                    grant(
                        table_privileges,
                        PgObjectType.TABLE,
                        table_name,
                        grantee,
                        grant_option=False,
                        schema=pg_schema,
                    ),
                    dry_run=dry_run,
                )


def create_acl_from_schemas(
    session,
    pg_schema,
    schemas,
    role,
    scopes,
    set_read_permissions,
    set_write_permissions,
    dry_run,
    create_roles,
    revoke,
):
    """Create and set the ACL for automatically generated roles based on Amsterdam Schema.
    Read permissions are granted to roles 'scope_X', where X are scopes found in Amsterdam Schema
    Write permissions are granted to roles 'write_Y', where Y are dataset ids,
    for all tables belonging to the dataset.
    Revoke old privileges before assigning new in case new privileges are more restrictive.
    """

    if revoke:
        if role == "AUTO":
            if isinstance(schemas, DatasetSchema):
                # for a single dataset
                _revoke_all_privileges_from_read_and_write_roles(
                    session, pg_schema, schemas, dry_run=dry_run
                )
            else:
                _revoke_all_privileges_from_read_and_write_roles(
                    session, pg_schema, dry_run=dry_run
                )
        else:
            if isinstance(schemas, DatasetSchema):
                # for a single dataset
                _revoke_all_privileges_from_role(
                    session, pg_schema, role, schemas, dry_run=dry_run
                )
            else:
                _revoke_all_privileges_from_role(session, pg_schema, role, dry_run=dry_run)

    if set_read_permissions:
        if isinstance(schemas, DatasetSchema):
            # for a single dataset
            set_dataset_read_permissions(
                session, pg_schema, schemas, role, scopes, dry_run, create_roles
            )
        else:
            for _dataset_name, dataset_schema in schemas.items():
                set_dataset_read_permissions(
                    session, pg_schema, dataset_schema, role, scopes, dry_run, create_roles
                )

    if set_write_permissions:
        if isinstance(schemas, DatasetSchema):
            # for a single dataset
            set_dataset_write_permissions(session, pg_schema, schemas, dry_run, create_roles)
        else:
            for _dataset_name, dataset_schema in schemas.items():
                set_dataset_write_permissions(
                    session, pg_schema, dataset_schema, dry_run, create_roles
                )


def _revoke_all_privileges_from_role(
    session, pg_schema, role, dataset_name=None, echo=True, dry_run=False
):
    status_msg = "Skipped" if dry_run else "Executed"
    if dataset_name:
        # for a single dataset
        revoke_statements = []
        for table in dataset_name.tables:
            revoke_statements.append(
                f"REVOKE ALL PRIVILEGES ON {pg_schema}.{table.db_name()} FROM {role}"
            )
        revoke_statement = ";".join(revoke_statements)
    else:
        revoke_statement = f"REVOKE ALL PRIVILEGES ON ALL TABLES IN {pg_schema} FROM {role}"
    sql_statement = (
        "DO $$ "
        "BEGIN "
        f"{revoke_statement}; "
        "EXCEPTION"
        " WHEN undefined_object"
        " THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE; "
        "END "
        "$$"
    )
    if echo:
        print(f"{status_msg} --> {revoke_statement}")
    if not dry_run:
        session.execute(sql_statement)


def _revoke_all_privileges_from_read_and_write_roles(
    session, pg_schema, dataset_name=None, echo=True, dry_run=False
):
    """Revoke all privileges that may have been previously granted to the scope_{} and write_{}
    roles. If dataset_name is provided, revoke only rights to the tables belonging to
    dataset_name.
    """

    status_msg = "Skipped" if dry_run else "Executed"
    # with engine.begin() as connection:
    result = session.execute(
        text(
            r"""
            SELECT rolname
            FROM pg_roles
            WHERE rolname LIKE 'scope\_%'
               OR rolname LIKE 'write\_%'
            """
        )
    )
    for rolname in result:
        if dataset_name:
            # for a single dataset
            revoke_statements = []
            for table in dataset_name.tables:
                revoke_statements.append(
                    f"REVOKE ALL PRIVILEGES ON {pg_schema}.{table.db_name()} FROM {rolname[0]}"
                )
            revoke_statement = ";".join(revoke_statements)
        else:
            revoke_statement = (
                f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {pg_schema} FROM {rolname[0]};"
            )
        if echo:
            print(f"{status_msg} --> {revoke_statement}")
        if not dry_run:
            session.execute(text(revoke_statement))  # .execution_options(autocommit=True))


def _execute_grant(session, grant_statement, echo=True, dry_run=False):
    """Wrap the grant statement in an anonymous code block to catch reasonable exceptions.
    We don't want to break out of the session just because a table, column, or user doesn't
    exist.
    """

    status_msg = "Skipped" if dry_run else "Executed"
    sql_statement = f"""
        DO
        $$
            BEGIN
                {grant_statement};
            EXCEPTION
                WHEN undefined_table OR undefined_column OR undefined_object
                    THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
            END
        $$
        """
    if echo:
        print(f"{status_msg} --> {grant_statement}")
    if not dry_run:
        session.execute(sql_statement)


def _create_role_if_not_exists(session, role, echo=True, dry_run=False):
    """Wrap the create role statement in an anonymous code block to be able to catch exception
    Don't break out of the session just because the role already exists
    """

    status_msg = "Skipped" if dry_run else "Executed"
    create_role_statement = f"CREATE ROLE {role}"
    if role not in existing_roles:
        sql_statement = (
            "DO $$ "
            "BEGIN "
            f"{create_role_statement}; "
            "EXCEPTION"
            " WHEN duplicate_object"
            " THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE; "
            "END "
            "$$"
        )
        if echo:
            print(f"{status_msg} --> {create_role_statement}")
        if not dry_run:
            session.execute(text(sql_statement))
        existing_roles.add(role)


def scope_to_role(scope):
    return f"scope_{scope.lower().replace('/', '_')}"
