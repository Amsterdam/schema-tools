"""Create GRANT statements to give roles very specific access to the database."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

from pg_grant import PgObjectType, parse_acl_item, query
from pg_grant.sql import grant, revoke
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from schematools.permissions import PUBLIC_SCOPE  # type: ignore [attr-defined]
from schematools.types import DatasetSchema, Scope

# Create a module-level logger, so calling code can
# configure the logger, if needed.
logger = logging.getLogger(__name__)

existing_roles = set()  # note: used as global cache!
existing_sequences = {}


def is_remote(table_name: str) -> bool:
    """Test if table_name refers a remote table.

    WARNING: UGLY HACK until 265311 is resolved and we
    can interrogate schematools to find out whether a table
    is remote.
    """
    return table_name.startswith("brp")


def introspect_permissions(engine: Engine, role: str) -> None:
    """Shows the table permissions."""
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    logger.info(
                        'role "%s" has privileges %s on table "%s"',
                        role,
                        ",".join(acl.privs),
                        schema_relation_info.name,
                    )


def revoke_permissions(engine: Engine, role: str, verbose: int = 0) -> None:
    """Revoke all privileges for the indicated role."""
    grantee = role
    schema_relation_infolist = query.get_all_table_acls(engine, schema="public")
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    if verbose:
                        logger.info(
                            'revoking ALL privileges of role "%s" on table "%s"',
                            role,
                            schema_relation_info.name,
                        )
                    revoke_statement = revoke(
                        "ALL", PgObjectType.TABLE, schema_relation_info.name, grantee
                    )
                    engine.execute(revoke_statement)


def apply_schema_and_profile_permissions(
    engine: Engine,
    pg_schema: str,
    ams_schema: DatasetSchema | dict[str, DatasetSchema],
    profiles: dict[str, Any],
    role: str,
    scope: str,
    set_read_permissions: bool = True,
    set_write_permissions: bool = True,
    dry_run: bool = False,
    create_roles: bool = False,
    revoke: bool = False,
    verbose: int = 0,
    additional_grants: tuple[str] = (),
) -> None:
    """Apply permissions for schema and profile."""
    SessionCls = sessionmaker(bind=engine)
    session = SessionCls()

    if verbose:

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            for notice in cursor.connection.notices:
                logger.info(notice.strip())

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
                verbose,
            )
        if profiles:
            profile_list = cast(list[dict[str, Any]], profiles.values())
            create_acl_from_profiles(engine, pg_schema, profile_list, role, scope, verbose)

        if additional_grants:
            set_additional_grants(session, pg_schema, dry_run, additional_grants, bool(verbose))

        session.commit()
    except Exception:
        session.rollback()
        logger.warning("Session rolled back")
        raise
    finally:
        session.close()


def create_acl_from_profiles(
    engine: Engine,
    pg_schema: str,
    profile_list: list[dict[str, Any]],
    role: str,
    scope: str,
    verbose: int = 0,
) -> None:
    """Create an ACL from profile list.

    NOTE: Rudimentary, not ready for production!
    """
    acl_list = query.get_all_table_acls(engine, schema=pg_schema)
    privileges = [
        "SELECT",
    ]
    grantee = role
    for profile in profile_list:
        if scope in profile["scopes"]:
            for dataset, _details in profile["schema_data"]["datasets"].items():
                for item in acl_list:
                    if item.name.startswith(dataset + "_"):
                        grant_statement = grant(
                            privileges,
                            PgObjectType.TABLE,
                            item.name,
                            grantee,
                            grant_option=False,
                            schema=pg_schema,
                        )
                        if verbose:
                            logger.info(grant_statement)
                        engine.execute(grant_statement)


def set_dataset_write_permissions(
    session: Session,
    pg_schema: str,
    ams_schema: DatasetSchema,
    dry_run: bool,
    create_roles: bool,
    echo: bool = False,
) -> None:
    """Sets write permissions for the indicated dataset."""
    grantee = f"write_{ams_schema.db_name}"
    if create_roles:
        _create_role_if_not_exists(session, grantee, dry_run=dry_run)

    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = table.db_name
        if is_remote(table_name):
            continue

        _execute_grant(
            session,
            grant(
                ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES"],
                PgObjectType.TABLE,
                table_name,
                grantee,
                grant_option=False,
                schema=pg_schema,
            ),
            echo=echo,
            dry_run=dry_run,
        )
        if table.is_autoincrement and (
            sequence_name := _get_sequence_name(session, table_name, "id")
        ):
            # Get PostgreSQL generated sequence for 'id' column that Django added.
            _execute_grant(
                session,
                grant(
                    ["USAGE"],
                    PgObjectType.SEQUENCE,
                    sequence_name,
                    grantee,
                    grant_option=False,
                    schema=pg_schema,
                ),
                echo=echo,
                dry_run=dry_run,
            )


@dataclass
class GrantParam:
    """Which object to give which permission.
    This intermediate object is used to collect results before making
    statements like ``GRANT SELECT ON <type> target TO <role>``.
    """

    privileges: list[str]
    target_type: PgObjectType  # often PgObjectType.TABLE
    target: str
    grantees: list[str]


def get_all_dataset_scopes(
    session: Session, ams_schema: DatasetSchema, role: str, scope: str
) -> list[GrantParam]:
    """Returns all scopes that should be applied to the tables of a dataset.

    Args:
        session: the SQL Alchemy session; used for getting the sequence names
        ams_schema: the amsterdam schema that needs to be processed
        role: the role that needs the grants that are calculated from the schema
            A special value `AUTO` can be used to apply grants for all scopes
        scope: only return grants for a specific scope, value can be empty string ("")

        The grants will be applied according to the configuration of the auth scopes
        in the amsterdam schema.
        If not auth scopes are defined, all tables get the `scope_openbaar` grant.
        If only the dataset has as scope `foo`, alle tables get the `scope_foo` grant.
        If a table has as scope `bar`, this overrules the dataset scope,
        so this table gets the `scope_bar` grant.
        If one or more fields have a scope, this scope overrules both the
        dataset and the table scope. The other fields in the same table
        get a `scope_openbaar` grant in that case.
        If a 1-N relation field has a scope, the foreign key field get the associated
        grant. In case this relation field is of type `object`` (e.g. for temporal fields),
        the additional columns (usually identificatie/volgnummer postfixed) get the same grant.
        If NM and nested relation fields (type `array` in the schema) have a scope `bar`
        the associated sub-table gets the grant `scope_bar`.

    Returns:
        all_scopes (list): Contains a list of scopes with priviliges and grants:

        [
            GrantParam(["SELECT"],           PgObjectType.TABLE, "table1", ["scope_openbaar"]),
            GrantParam(["SELECT (columnA)"], PgObjectType.TABLE, "table2", ["scope_openbaar"]),
            GrantParam(["SELECT (columnB)"], PgObjectType.TABLE, "table2", ["scope_A", "scope_B"]),
        ]
    """

    def _fetch_grantees(scopes: frozenset[str] | frozenset[Scope]) -> list[str]:
        if role == "AUTO":
            return [scope_to_role(_scope) for _scope in scopes]
        elif any(s.id == scope if isinstance(s, Scope) else s == scope for s in scopes):
            return [role]
        else:
            return []

    all_scopes = []
    dataset_scopes = ams_schema.scopes
    PUBLIC_SCOPE_OBJECT = Scope({"id": PUBLIC_SCOPE})
    public_scopes = {PUBLIC_SCOPE_OBJECT, PUBLIC_SCOPE}

    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        table_name = table.db_name
        if is_remote(table_name):
            continue

        table_scopes = (table.scopes - public_scopes) or dataset_scopes
        fields = [
            field
            for field in table.get_fields(include_subfields=True)
            if not field.type.endswith("#/definitions/schema")
        ]

        # First process all fields, to know if any fields has a non-public scope
        column_scopes = {}
        for field in fields:
            # Object type relations have subfields, in that case
            # the auth scope on the relation is leading.
            field_scopes = field.scopes - public_scopes
            if field.is_subfield:
                field_scopes = (field.parent_field.scopes - public_scopes) or field_scopes

            if field_scopes:
                column_scopes[field.db_name] = field_scopes

        if column_scopes:
            for field in fields:
                if field.nm_relation or field.is_nested_table:
                    # field is not in view when nm or nested
                    continue

                column_name = field.db_name
                grantees = _fetch_grantees(column_scopes.get(column_name, table_scopes))
                all_scopes.append(
                    GrantParam(
                        # NB. space after SELECT is significant!
                        privileges=[f"SELECT ({column_name})"],
                        target_type=PgObjectType.TABLE,
                        target=table_name,
                        grantees=grantees,
                    )
                )
                # Get PostgreSQL generated sequence for 'id' column that Django added.
                if (
                    field.is_primary
                    and table.is_autoincrement
                    and (sequence_name := _get_sequence_name(session, table_name, "id"))
                ):
                    all_scopes.append(
                        GrantParam(
                            privileges=["SELECT"],
                            target_type=PgObjectType.SEQUENCE,
                            target=sequence_name,
                            grantees=grantees,
                        )
                    )
        else:
            if table_name not in all_scopes:
                grantees = _fetch_grantees(table_scopes)
                all_scopes.append(
                    GrantParam(
                        privileges=["SELECT"],
                        target_type=PgObjectType.TABLE,
                        target=table_name,
                        grantees=grantees,
                    )
                )
                if table.is_autoincrement and (
                    sequence_name := _get_sequence_name(session, table_name, "id")
                ):
                    all_scopes.append(
                        GrantParam(
                            privileges=["SELECT"],
                            target_type=PgObjectType.SEQUENCE,
                            target=sequence_name,
                            grantees=grantees,
                        )
                    )

    return all_scopes


def set_dataset_read_permissions(
    session: Session,
    pg_schema: str,
    ams_schema: DatasetSchema,
    role: str,
    scope: str,
    dry_run: bool,
    create_roles: bool,
    echo: bool = False,
) -> None:
    """Sets read permissions for the indicated dataset.

    Args:
        session: SQLAlchemy type session
        pg_schema: schema in the postgres database
        ams_schema: the amsterdam schema that needs to be processed
        role: the role that needs the grants that are calculated from the schema
            A special value `AUTO` can be used to apply grants for all scopes
        scope: only apply grants for a specific scope, value can be empty string ("")
        dry_run: do not apply the grants
        create_roles: boolean indicating that if certain roles are not in the postgres db,
            these roles need to be created.

        The grants will be applied according to the configuration of the auth scopes
        in the amsterdam schema.
        If not auth scopes are defined, all tables get the `scope_openbaar` grant.
        If only the dataset has as scope `foo`, alle tables get the `scope_foo` grant.
        If a table has as scope `bar`, this overrules the dataset scope,
        so this table gets the `scope_bar` grant.
        If one or more fields have a scope, this scope overrules both the
        dataset and the table scope. The other fields in the same table
        get a `scope_openbaar` grant in that case.
        If a 1-N relation field has a scope, the foreign key field get the associated
        grant. In case this relation field is of type `object`` (e.g. for temporal fields),
        the additional columns (usually identificatie/volgnummer postfixed) get the same grant.
        If NM and nested relation fields (type `array` in the schema) have a scope `bar`
        the associated sub-table gets the grant `scope_bar`.
    """
    grantee = None if role == "AUTO" else role
    if create_roles and grantee:
        _create_role_if_not_exists(session, grantee, dry_run=dry_run)

    all_grants = get_all_dataset_scopes(session, ams_schema, role, scope)
    for grant_param in all_grants:
        # For global and specific columns:
        for _grantee in grant_param.grantees:
            if create_roles:
                _create_role_if_not_exists(session, _grantee, dry_run=dry_run)
            _execute_grant(
                session,
                grant(
                    grant_param.privileges,
                    type=grant_param.target_type,
                    target=grant_param.target,
                    grantee=_grantee,
                    grant_option=False,
                    schema=pg_schema,
                ),
                echo=echo,
                dry_run=dry_run,
            )


def set_additional_grants(
    session: Session,
    pg_schema: str,
    dry_run: bool,
    additional_grants: tuple[str],
    echo: bool = False,
) -> None:
    """Sets additional grants

    Args:
        session: SQLAlchemy type session
        pg_schema: schema in the postgres database
        dry_run: do not apply the grants
        additional_grants: tuple with the following structure:
            <table_name>:<privilege_1>[,<privilege_n>]*;<grantee_1>[,grantee_n]*
        echo: show output
    """

    for additional_grant in additional_grants:
        try:
            table_name, grant_params = additional_grant.split(":")
            privileges_str, grantees_str = grant_params.split(";")
            privileges = privileges_str.split(",")
            grantees = grantees_str.split(",")
        except ValueError:
            logger.error(
                "Incorrect grant definition: `%r`,"
                "grant should have format"
                "<table_name>:<privilege_1>[,<privilege_n>]*;<grantee_1>[,grantee_n]*",
                additional_grant,
            )
            raise  #  re-raise to trigger rollback
        for _grantee in grantees:
            _execute_grant(
                session,
                grant(
                    privileges,
                    PgObjectType.TABLE,
                    table_name,
                    _grantee,
                    grant_option=False,
                    schema=pg_schema,
                ),
                echo=echo,
                dry_run=dry_run,
            )


def create_acl_from_schemas(
    session: Session,
    pg_schema: str,
    schemas: DatasetSchema | dict[str, DatasetSchema],
    role: str,
    scope: str,
    set_read_permissions: bool,
    set_write_permissions: bool,
    dry_run: bool,
    create_roles: bool,
    revoke: bool,
    verbose: int = 0,
) -> None:
    """Create and set the ACL for automatically generated roles based on Amsterdam Schema.

    Read permissions are granted to roles 'scope_X', where X are scopes found in Amsterdam Schema
    Write permissions are granted to roles 'write_Y', where Y are dataset ids,
    for all tables belonging to the dataset.
    Revoke old privileges before assigning new in case new privileges are more restrictive.
    """
    if revoke:
        # For a single dataset, or all tables?
        revoke_dataset = schemas if isinstance(schemas, DatasetSchema) else None
        if role == "AUTO":
            # All roles
            _revoke_all_privileges_from_read_and_write_roles(
                session, pg_schema, revoke_dataset, dry_run=dry_run, echo=bool(verbose)
            )
        else:
            # Only for a single role
            _revoke_all_privileges_from_role(
                session, pg_schema, role, revoke_dataset, dry_run=dry_run, echo=bool(verbose)
            )

    datasets = [schemas] if isinstance(schemas, DatasetSchema) else schemas.values()
    for dataset in datasets:
        if set_read_permissions:
            set_dataset_read_permissions(
                session,
                pg_schema,
                dataset,
                role,
                scope,
                dry_run,
                create_roles,
                echo=bool(verbose),
            )

        if set_write_permissions:
            set_dataset_write_permissions(
                session, pg_schema, dataset, dry_run, create_roles, echo=bool(verbose)
            )


def _revoke_all_privileges_from_role(
    session: Session,
    pg_schema: str,
    role: str,
    dataset: DatasetSchema | None = None,
    echo: bool = True,
    dry_run: bool = False,
) -> None:
    status_msg = "Skipped" if dry_run else "Executed"
    if dataset:
        # for a single dataset
        revoke_statements = []
        for table in dataset.tables:
            revoke_statements.append(
                f"REVOKE ALL PRIVILEGES ON {pg_schema}.{table.db_name} FROM {role}"
            )
        revoke_statement = ";".join(revoke_statements)
    else:
        revoke_statement = f"REVOKE ALL PRIVILEGES ON ALL TABLES IN {pg_schema} FROM {role}"
    sql_statement = f"""
        DO $$
        BEGIN
        f{revoke_statement};
        EXCEPTION
         WHEN undefined_object
         THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
        END
        $$
    """
    if echo:
        logger.info("%s --> %s", status_msg, revoke_statement)
    if not dry_run:
        session.execute(sql_statement)


def _revoke_all_privileges_from_read_and_write_roles(
    session: Session,
    pg_schema: str,
    dataset: DatasetSchema | None = None,
    echo: bool = True,
    dry_run: bool = False,
) -> None:
    """Revoke all privileges that may have been previously granted.

    This is about grants to the scope_* and write_* roles.
    If dataset is provided, revoke only rights to the tables belonging to
    dataset.
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
        if dataset:
            # for a single dataset
            revoke_statements = []
            for table in dataset.tables:
                revoke_statements.append(
                    f"REVOKE ALL PRIVILEGES ON {pg_schema}.{table.db_name} FROM {rolname[0]}"
                )
                if table.is_autoincrement and (
                    sequence_name := _get_sequence_name(session, table.db_name, "id")
                ):
                    revoke_statements.append(
                        f"REVOKE ALL PRIVILEGES ON SEQUENCE {pg_schema}.{sequence_name}"
                        f" FROM {rolname[0]}"
                    )
        else:
            revoke_statements = [
                f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {pg_schema} FROM {rolname[0]}",
                f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {pg_schema} FROM {rolname[0]}",
            ]

        revoke_statement = ";".join(revoke_statements)
        revoke_block_statement = f"""
            DO
            $$
                BEGIN
                    {revoke_statement};
                EXCEPTION
                    WHEN insufficient_privilege THEN
                        RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
                END
            $$
        """

        if echo:
            logger.info("%s --> %s", status_msg, revoke_statement)
        if not dry_run:
            session.execute(revoke_block_statement)


def _execute_grant(
    session: Session, grant_statement: str, echo: bool = True, dry_run: bool = False
) -> None:
    status_msg = "Skipped" if dry_run else "Executed"
    sql_statement = f"""
        DO
        $$
            BEGIN
                {grant_statement};
            EXCEPTION
              WHEN undefined_table THEN
                RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
            END
        $$
        """
    if echo:
        logger.info("%s --> %s", status_msg, grant_statement)
    if not dry_run:
        session.execute(sql_statement)


def _create_role_if_not_exists(
    session: Session, role: str, echo: bool = True, dry_run: bool = False
) -> None:
    """Wrap the create role statement in an anonymous code block.

    Reason is to be able to catch exceptions.
    Don't break out of the session just because the role already exists
    """
    status_msg = "Skipped" if dry_run else "Executed"
    create_role_statement = f"CREATE ROLE {role}"
    if role not in existing_roles:
        sql_statement = f"""
            DO $$
            BEGIN
            {create_role_statement};
            EXCEPTION
             WHEN duplicate_object
             THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
             WHEN undefined_object
             THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
            END
            $$
        """
        if echo:
            logger.info("%s --> %s", status_msg, create_role_statement)
        if not dry_run:
            session.execute(text(sql_statement))
        existing_roles.add(role)


def _get_sequence_name(session: Session, table_name: str, column: str) -> str | None:
    key = (table_name, column)
    try:
        # Can't use lru_cache() as it caches 'session' too.
        return existing_sequences[key]
    except KeyError:
        row = session.execute(
            text("SELECT pg_get_serial_sequence(:table, :column)"),
            {"table": table_name, "column": column},
        ).first()
        value = (
            row[0].replace("public.", "").replace('"', "")
            if row is not None and row[0] is not None
            else None
        )
        if not value:
            logger.debug("No sequence found for %s.%s", table_name, column)
        existing_sequences[key] = value
        return value


def scope_to_role(scope: str | Scope) -> str:
    """Return rolename for the postgres database."""
    id = scope.id if isinstance(scope, Scope) else scope
    return f"scope_{id.lower().replace('/', '_')}"
