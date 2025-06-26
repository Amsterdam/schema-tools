"""Create GRANT statements to give roles very specific access to the database."""

from __future__ import annotations

import logging

from pg_grant import PgObjectType, parse_acl_item, query
from pg_grant.sql import _Grant, grant, revoke
from sqlalchemy import Connection, event, text
from sqlalchemy.engine import Engine

from schematools.permissions import PUBLIC_SCOPE
from schematools.types import (
    DatasetFieldSchema,
    DatasetSchema,
    DatasetTableSchema,
    PermissionLevel,
    ProfileSchema,
    Scope,
)

# Create a module-level logger, so calling code can
# configure the logger, if needed.
logger = logging.getLogger(__name__)

existing_roles = set()  # note: used as global cache!
existing_sequences = {}

PUBLIC_SCOPE_OBJECT = Scope({"id": PUBLIC_SCOPE})
PUBLIC_SCOPES = {PUBLIC_SCOPE_OBJECT, PUBLIC_SCOPE}


def introspect_permissions(engine: Engine, role: str) -> None:
    """Shows the table permissions."""
    with engine.connect() as conn:
        schema_relation_infolist = query.get_all_table_acls(conn, schema="public")

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
    with engine.connect() as conn:
        schema_relation_infolist = query.get_all_table_acls(conn, schema="public")

    revoke_statements = []
    for schema_relation_info in schema_relation_infolist:
        if schema_relation_info.acl:
            acl_list = [parse_acl_item(item) for item in schema_relation_info.acl]
            for acl in acl_list:
                if acl.grantee == role:
                    revoke_statements.append(
                        revoke("ALL", PgObjectType.TABLE, schema_relation_info.name, grantee)
                    )

    with engine.begin() as conn:
        for revoke_statement in revoke_statements:
            if verbose:
                logger.info(
                    'revoking ALL privileges of role "%s" on table "%s"',
                    role,
                    revoke_statement.target,
                )

            conn.execute(revoke_statement)


def apply_schema_and_profile_permissions(
    engine: Engine,
    schemas: DatasetSchema | dict[str, DatasetSchema],
    profiles: list[ProfileSchema] | None,
    *,
    only_role: str | None = None,
    only_scope: str | None = None,
    set_read_permissions: bool = True,
    set_write_permissions: bool = True,
    dry_run: bool = False,
    create_roles: bool = False,
    revoke: bool = False,
    verbose: int = 0,
    additional_grants: tuple[str] = (),
    all_scopes: list[Scope] | None = None,
) -> None:
    """Apply permissions for schema and profile.

    Read permissions are granted to roles 'scope_X', where X are scopes found in Amsterdam Schema.
    Write permissions are granted to roles 'write_Y', where Y are dataset ids,
    for all tables belonging to the dataset.
    Revoke old privileges before assigning new in case new privileges are more restrictive.
    """
    datasets = {schemas.id: schemas} if isinstance(schemas, DatasetSchema) else schemas

    if verbose:
        event.listen(engine, "before_cursor_execute", _before_cursor_execute)

    with engine.connect() as conn:
        try:
            if datasets:
                if revoke:
                    # Remove all privileges first
                    revoke_dataset = schemas if isinstance(schemas, DatasetSchema) else None
                    revoke_schema_permissions(
                        conn, revoke_dataset, only_role, dry_run, verbose=verbose
                    )

                if create_roles:
                    for scope in all_scopes or []:
                        role = _scope_to_role(scope)
                        filtered_role = f"{role}.filtered"
                        _create_role_if_not_exists(conn, role, verbose=verbose, dry_run=dry_run)
                        _create_role_if_not_exists(
                            conn, filtered_role, inherits=role, verbose=verbose, dry_run=dry_run
                        )

                # Apply privileges for all datasets, or the selected dataset.
                apply_schema_permissions(
                    conn,
                    datasets,
                    only_role,
                    only_scope,
                    set_read_permissions,
                    set_write_permissions,
                    dry_run=dry_run,
                    create_roles=create_roles,
                    verbose=verbose,
                )

                if profiles:
                    apply_profile_permissions(
                        conn,
                        profiles,
                        datasets,
                        only_role,
                        only_scope,
                        dry_run=dry_run,
                        create_roles=create_roles,
                        verbose=verbose,
                    )

            if additional_grants:
                apply_additional_grants(
                    conn, additional_grants, dry_run=dry_run, create_roles=False, verbose=verbose
                )

            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning("Session rolled back")
            raise
        finally:
            if verbose:
                event.remove(engine, "before_cursor_execute", _before_cursor_execute)


def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Report notices raised by the 'RAISE NOTICE' statements."""
    raw_connection = cursor.connection
    if not raw_connection._notice_handlers:
        raw_connection.add_notice_handler(_on_cursor_notice)


def _on_cursor_notice(diagnostic):
    logger.info(diagnostic.message_primary)


def _collect_dataset_write_grants(conn: Connection, ams_schema: DatasetSchema) -> list[_Grant]:
    """Sets write permissions for the indicated dataset."""
    grantee = f"write_{ams_schema.db_name}"
    all_grants = []
    for table in ams_schema.get_tables(include_nested=True, include_through=True):
        all_grants.extend(
            _build_table_grants(
                conn,
                table,
                privileges=["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES"],
                grantees=[grantee],
            )
        )
    return all_grants


def _collect_dataset_grants(
    conn: Connection,
    dataset: DatasetSchema,
    only_role: str | None = None,
    only_scope: str | None = None,
) -> list[_Grant]:
    """Returns all scopes that should be applied to the tables of a dataset.

    Args:
        session: the SQL Alchemy session; used for getting the sequence names
        dataset: the amsterdam schema that needs to be processed
        only_role: only return grants for a specific role, none means all roles.
        only_scope: only return grants for a specific scope, none means all scopes.

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
        all_scopes (list): Contains a list of scopes with privileges and grants:

        [
            grant(["SELECT"],           PgObjectType.TABLE, "table1", "scope_openbaar"),
            grant(["SELECT (columnA)"], PgObjectType.TABLE, "table2", "scope_openbaar"),
            grant(["SELECT (columnB)"], PgObjectType.TABLE, "table2", "scope_A"),
            grant(["SELECT (columnB)"], PgObjectType.TABLE, "table2", "scope_B"]),
        ]
    """
    grants = []
    for table in dataset.get_tables(include_nested=True, include_through=True):
        table_scopes = (table.scopes - PUBLIC_SCOPES) or dataset.scopes
        fields = [
            field
            for field in table.get_fields(include_subfields=True)
            if not field.type.endswith("#/definitions/schema")
        ]

        # First process all fields, to know if any fields has a non-public scope
        column_scopes = _get_column_level_scopes(fields)
        if column_scopes:
            # When some fields have a specific scope,
            # grant statements need to be generated for each individual field.
            # After all, "GRANT SELECT ON 'table'" would bypass field-level access.
            for field in fields:
                if field.nm_relation or field.is_nested_table:
                    # field is not in view when nm or nested
                    continue

                # Column-level read permissions
                scopes = column_scopes.get(field.db_name, table_scopes)
                if grantees := _filter_grantees(scopes, only_role, only_scope):
                    grants.extend(_build_field_grants(conn, field, grantees))
        elif grantees := _filter_grantees(table_scopes, only_role, only_scope):
            # Table-level read permissions
            grants.extend(_build_table_grants(conn, table, ["SELECT"], grantees))

    return grants


def _collect_profile_grants(
    conn: Connection,
    profile: ProfileSchema,
    datasets: dict[str, DatasetSchema],
    only_role: str | None = None,
    only_scope: str | None = None,
) -> list[_Grant]:
    """Tell which grants a profile should given.

    Args:
        conn: SQLAlchemy connection.
        profile: The profile to calculate GRANT statements for
        datasets: Associated datasets touched by the profile.
        only_role: The role to filter grants by.
        only_scope: The scope to filter grants by.
    """
    grants = []
    grantees = _filter_grantees(profile.scopes, only_role=only_role, only_scope=only_scope)
    if not grantees:
        return []

    for profile_dataset in profile.datasets.values():
        dataset = datasets[profile_dataset.id]

        if profile_dataset.permissions.level >= PermissionLevel.LETTERS:
            # READ access on dataset level.
            # Giving read access to a whole dataset.
            for table in dataset.get_tables(include_nested=True, include_through=True):
                grants.extend(_build_table_grants(conn, table, ["SELECT"], grantees))
        else:
            dataset_tables = {table.id: table for table in dataset.tables}
            for profile_table in profile_dataset.tables.values():
                table = dataset_tables[profile_table.id]
                table_grantees = (
                    [f"{s}.filtered" for s in grantees]
                    if profile_table.mandatory_filtersets
                    else grantees
                )

                if profile_table.permissions:
                    # READ access on table level.
                    grants.extend(_build_table_grants(conn, table, ["SELECT"], table_grantees))
                elif profile_table.fields:
                    # READ access on field level.
                    fields_by_name = {
                        (f"{f.parent_field.id}.{f.id}" if f.parent_field is not None else f.id): f
                        for f in table.get_fields(include_subfields=True)
                    }

                    for field_name, field_permissions in profile_table.fields.items():
                        if field_permissions:  # sanity check
                            grants.extend(
                                _build_field_grants(
                                    conn, fields_by_name[field_name], table_grantees
                                )
                            )
    return grants


def _get_column_level_scopes(fields: list[DatasetFieldSchema]) -> dict[str, frozenset[Scope]]:
    """Tell whether there are fields that have an explicit scope."""
    column_scopes = {}
    for field in fields:
        # Object type relations have subfields, in that case
        # the auth scope on the relation is leading.
        field_scopes = field.scopes - PUBLIC_SCOPES
        if field.is_subfield:
            field_scopes = (field.parent_field.scopes - PUBLIC_SCOPES) or field_scopes

        if field_scopes:
            column_scopes[field.db_name] = field_scopes

    return column_scopes


def _build_table_grants(
    conn: Connection,
    table: DatasetTableSchema,
    privileges: list[str],
    grantees: list[str],
) -> list[_Grant]:
    """Build the SELECT grants for accessing a full table."""
    grants = [
        grant(
            privileges,
            type=PgObjectType.TABLE,
            target=table.db_name,
            grantee=grantee,
            schema="public",
        )
        for grantee in grantees
    ]

    if sequence_name := _get_sequence_name(conn, table):
        grants.extend(
            grant(
                ["USAGE" if "INSERT" in privileges else "SELECT"],
                type=PgObjectType.SEQUENCE,
                target=sequence_name,
                grantee=grantee,
                schema="public",
            )
            for grantee in grantees
        )
    return grants


def _build_field_grants(
    conn: Connection, field: DatasetFieldSchema, grantees: list[str]
) -> list[_Grant]:
    """Build the SELECT grants for accessing a field."""
    table = field.table
    grants = [
        grant(
            # NB. space after SELECT is significant!
            privileges=[f"SELECT ({field.db_name})"],
            type=PgObjectType.TABLE,
            target=table.db_name,
            grantee=grantee,
            schema="public",
        )
        for grantee in grantees
    ]

    # Get PostgreSQL generated sequence for 'id' column that Django added.
    if field.is_primary and (sequence_name := _get_sequence_name(conn, table)):
        grants.extend(
            grant(
                privileges=["SELECT"],
                type=PgObjectType.SEQUENCE,
                target=sequence_name,
                grantee=grantee,
                schema="public",
            )
            for grantee in grantees
        )
    return grants


def _collect_additional_grants(additional_grants: tuple[str]) -> list[_Grant]:
    """Parse the additional grants syntax into GRANT statements."""
    all_grants = []
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

        all_grants.extend(
            grant(
                privileges,
                PgObjectType.TABLE,
                table_name,
                _grantee,
                schema="public",
            )
            for _grantee in grantees
        )

    return all_grants


def apply_schema_permissions(
    conn: Connection,
    datasets: dict[str, DatasetSchema],
    only_role: str | None,
    only_scope: str | None,
    set_read_permissions: bool,
    set_write_permissions: bool,
    dry_run: bool,
    create_roles: bool = False,
    verbose: int = 0,
) -> None:
    """Create and set the ACL for automatically generated roles based on Amsterdam Schema.

    Read permissions are granted to roles 'scope_X', where X are scopes found in Amsterdam Schema
    Write permissions are granted to roles 'write_Y', where Y are dataset ids,
    for all tables belonging to the dataset.
    Revoke old privileges before assigning new in case new privileges are more restrictive.
    """
    for dataset in datasets.values():
        all_grants = (
            _collect_dataset_grants(conn, dataset, only_role=only_role, only_scope=only_scope)
            if set_read_permissions
            else []
        )
        if set_write_permissions:
            all_grants.extend(_collect_dataset_write_grants(conn, dataset))

        _execute_grants(
            conn,
            all_grants,
            dry_run=dry_run,
            create_roles=create_roles,
            verbose=verbose,
        )


def apply_profile_permissions(
    conn: Connection,
    profiles: list[ProfileSchema],
    datasets: dict[str, DatasetSchema],
    only_role: str | None = None,
    only_scope: str | None = None,
    dry_run: bool = False,
    create_roles: bool = False,
    verbose: int = 0,
) -> None:
    """Create an ACL from profile list."""
    dataset_ids = set(datasets.keys())
    profiles = [
        p
        for p in profiles
        if dataset_ids.intersection(p.datasets.keys())
        and only_scope is None
        or only_scope in p.scopes
    ]
    if not profiles:
        return

    # Retrieve all grants for the profiles
    all_grants = []
    for profile in profiles:
        all_grants.extend(
            _collect_profile_grants(
                conn, profile, datasets, only_role=only_role, only_scope=only_scope
            )
        )

    _execute_grants(conn, all_grants, dry_run=dry_run, create_roles=create_roles, verbose=verbose)


def apply_additional_grants(
    conn: Connection,
    additional_grants: tuple[str],
    dry_run: bool = False,
    create_roles: bool = False,
    verbose: int = 0,
):
    """
    Parse manaully defined grants.

    Args:
        additional_grants: tuple with the following structure:
            <table_name>:<privilege_1>[,<privilege_n>]*;<grantee_1>[,grantee_n]*
    """
    all_grants = _collect_additional_grants(additional_grants)
    _execute_grants(conn, all_grants, dry_run=dry_run, create_roles=create_roles, verbose=verbose)


def revoke_schema_permissions(
    conn: Connection,
    only_dataset: DatasetSchema | None = None,
    only_role: str | None = None,
    dry_run: bool = False,
    verbose: int = 1,
):
    """Revoke all privileges that may have been previously granted.

    This is about grants to the scope_* and write_* roles.
    If dataset is provided, revoke only rights to the tables belonging to
    dataset.
    """
    pg_schema = "public"
    db_role_names = _get_all_role_names(conn) if only_role is None else [only_role]

    revoke_statements = []
    if only_dataset:
        # for a single dataset
        for table in only_dataset.tables:
            revoke_statements.extend(
                f'REVOKE ALL PRIVILEGES ON "{pg_schema}.{table.db_name}" FROM "{role_name}"'
                for role_name in db_role_names
            )
            if sequence_name := _get_sequence_name(conn, table):
                revoke_statements.extend(
                    f'REVOKE ALL PRIVILEGES ON SEQUENCE "{pg_schema}.{sequence_name}"'
                    f' FROM "{role_name}"'
                    for role_name in db_role_names
                )
    else:
        # For all datasets
        revoke_statements = [
            f'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {pg_schema} FROM "{role_name}"'
            for role_name in db_role_names
        ] + [
            f'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {pg_schema} FROM "{role_name}"'
            for role_name in db_role_names
        ]

    # Execute all for this role
    for statement in revoke_statements:
        _execute_grant(conn, statement, verbose=verbose, dry_run=dry_run)


def _execute_grants(
    conn,
    all_grants: list[_Grant],
    dry_run: bool,
    create_roles: bool = False,
    verbose: int = 0,
) -> None:
    """Apply the collected grant statements."""
    for grant_statement in all_grants:
        # For global and specific columns:
        if create_roles:
            role = grant_statement.grantee
            if "UPDATE" in grant_statement.privileges or role.startswith("write_"):
                # Write users don't need .filtered
                _create_role_if_not_exists(conn, role, verbose=verbose, dry_run=dry_run)
            else:
                # Make sure both the regular and ".filtered" variant exists.
                # Users with direct connection to the database only inherit from the regular roles.
                # The DSO-API code can switch to the .filtered version,
                # which grants extra access to tables that have mandatoryFilterSets.
                if role.endswith(".filtered"):
                    app_role = role
                    role = role[: -len(".filtered")]
                else:
                    app_role = f"{role}.filtered"

                _create_role_if_not_exists(conn, role, verbose=verbose, dry_run=dry_run)
                _create_role_if_not_exists(
                    conn, app_role, inherits=role, verbose=verbose, dry_run=dry_run
                )

        _execute_grant(conn, grant_statement, verbose=verbose, dry_run=dry_run)


def _execute_grant(
    conn: Connection, grant_statement: _Grant, verbose: int = 1, dry_run: bool = False
) -> None:
    status_msg = "Skipped" if dry_run else "Executed"
    if verbose:
        logger.info("%s --> %s", status_msg, grant_statement)
    if not dry_run:
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
        conn.execute(text(sql_statement))


def _create_role_if_not_exists(
    conn: Connection,
    role: str,
    inherits: str | None = None,
    verbose: int = 1,
    dry_run: bool = False,
) -> None:
    """Wrap the create role statement in an anonymous code block.

    Reason is to be able to catch exceptions.
    Don't break out of the session just because the role already exists
    """
    if role not in existing_roles:
        create_role_statement = f'CREATE ROLE "{role}"'
        if inherits:
            # PostgreSQL note, there are 2 syntax versions:
            # - "CREATE ROLE child IN ROLE parent" - this adds a member.
            # - "CREATE ROLE parent ROLE child"    - this declares a group, with initial members.
            create_role_statement = f'{create_role_statement} IN ROLE "{inherits}"'

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
        if verbose:
            status_msg = "Skipped" if dry_run else "Executed"
            logger.info("%s --> %s", status_msg, create_role_statement)
        if not dry_run:
            conn.execute(text(sql_statement))
        existing_roles.add(role)


def _get_sequence_name(conn: Connection, table: DatasetTableSchema) -> str | None:
    """Find the autoincrement sequence of a table."""
    if not table.is_autoincrement:
        return None

    column = table.identifier_fields[0].db_name  # always 1 field for autoincrement.
    key = (table.db_name, column)
    try:
        # Can't use lru_cache() as it caches 'session' too.
        return existing_sequences[key]
    except KeyError:
        row = conn.execute(
            text("SELECT pg_get_serial_sequence(:table, :column)"),
            {"table": table.db_name, "column": column},
        ).first()
        value = (
            row[0].replace("public.", "").replace('"', "")
            if row is not None and row[0] is not None
            else None
        )
        if not value:
            logger.debug("No sequence found for %s.%s", table.db_name, column)
        existing_sequences[key] = value
        return value


def _get_all_role_names(conn: Connection) -> list[str]:
    """Find all roles currently used in the database."""
    result = conn.execute(
        text(
            r"""
            SELECT rolname
            FROM pg_roles
            WHERE rolname LIKE 'scope\_%'
               OR rolname LIKE 'write\_%'
            """
        )
    )
    return [row[0] for row in result]


def _filter_grantees(
    scopes: frozenset[Scope] | frozenset[str], only_role: str | None, only_scope: str | None
) -> list[str]:
    """Determine which roles to assign for the scopes.
    This limits the granted scopes to the limitations given by CLI parameters.

    NOTE: The 'profiles' currently give a list of string scope names,
    while the 'dataset' objects already reference Scope objects.
    """
    if only_role is None:
        return [_scope_to_role(_scope) for _scope in scopes]
    elif any(s.id == only_scope if isinstance(s, Scope) else s == only_scope for s in scopes):
        return [only_role]
    else:
        return []


def _scope_to_role(scope: Scope | str) -> str:
    """Return rolename for the postgres database."""
    id = scope.id if isinstance(scope, Scope) else scope
    return f"scope_{id.lower().replace('/', '_')}"
