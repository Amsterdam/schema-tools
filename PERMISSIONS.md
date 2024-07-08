# Permissions at the Database Level

Read authorization for database roles is defined by Amsterdam Schema using the `auth` property.

An `auth` property may be set at dataset, table, or field/column level, and contains a string or
list of strings of scopes.

The following rules are applied to determine which role has read (`SELECT`) access to what database column:

- For each scope `x` in Amsterdam Schema, an associated postgres role is created with the name `scope_x`.
- If there is no `auth` specified on dataset, table, or column level, read access is given to
  the default role `scope_openbaar`. This scope means the data is publicly readable.
- If a dataset has an `auth` scope specified, the associated role is given read access to
  all columns in all tables within the dataset, unless it is overridden by an `auth` scope
  on table or column level.
- If a table has an `auth` scope specified, the associated role is given read access to
  the whole table, except for those columns that have their own `auth` scope.
  Any scope defined at the dataset level is in this case being overruled, and read access to this
  particular table denied.
- If a column has an `auth` role specified, the associated role is given read acces to the column.
  Any scope defined at the dataset or table level is in this case being overruled, and read access
  to this particular column denied.

In summary, lower level scopes overrule higher level scopes, and priviliges granted to lower level scopes
are taken away from higher level scopes. The field/column level is lowest, dataset is highest. A
typical use case is a dataset with a broad scope and accessible to a large group of people, while
certain privacy sensitive tables or columns have a more restricted access with a more narrow scope.

For each dataset, a write role is created with the name `write_{dataset.id}`, with `INSERT`, `UPDATE`, `DELETE`,
`TRUNCATE`, and `REFERENCES` priviliges. These roles may be granted to a particular user to allow writing
data to existing tables. It should be noted that the write roles do not have the `SELECT` privilege,
since that would bypass the previously discussed mechanism for assigning read privileges by
Amsterdam Schema.

For a user to update data in dataset `X` with scope `Y`, she would need to have been granted both the
`write_X` and `scope_Y` roles, because updating requires both `UPDATE` and `SELECT` privileges.


## CLI

For a complete overview of CLI options use the `--help` option.

Examples of CLI usage:

### Typical use case to create and/or update all read and write roles
```shell script
schema permissions apply --auto --revoke --create-roles --execute
````

### Postgres user authorization
```shell script
schema permissions apply tests/files/gebieden_auth.json tests/files/profiles/gebieden_test.json level_b_user LEVEL/B
```
Here, `tests/files/gebieden_auth.json` is a local file containing an example Amsterdam Schema,
`tests/files/profiles/gebieden_test.json` is a local file containing an example profile.
`level_b_user` is an existing postgres role, which will be granted read priviliges in accordance with scope `LEVEL/B`.

For a dry run without actually executing the GRANT statements, add the option `--dry-run`.
The GRANT statements will be printed to console for inspection.

Profile authorization is still under development, and may be left out by specifying `NONE` instead of a filename.

To grant permissions according to a complete Amsterdam Schema at `SCHEMA_URL`, specify `ALL` instead of a filename:
```shell script
schema permissions apply ALL NONE johan BRP/R
```
Here, `johan` is an existing postgres role, and `BRP/R` is a scope for which `johan` is being authorized.
To only grant permissions for a specific dataset in the Amsterdam Schema at `SCHEMA_URL`, specify that dataset:

```shell script
schema permissions apply brp NONE johan BRP/R

```

### Introspection of permissions

```shell script
schema permissions introspect brp_r
```
Lists all table permissions for postgres role brp_r

### Revoking permissions
```shell script
schema permissions revoke brp_r
```
Revokes all table permissions for postgres role brp_r

## Tests

Tests may be run from the schema-tools root directory:
```shell script
pytest tests/test_permissions.py
```
