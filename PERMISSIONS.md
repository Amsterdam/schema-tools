# Permissions at the Database Level

Authorization is set in two different ways:
- Amsterdam Schema Authorization.  An "auth" field may be set at dataset, tabel, or field level, and contains a string or list of strings of scopes.
- Profile Authorization (in progress).

This document describes how the CLI can be used to set permissions at the database level using these two methods.


## CLI

Examples of CLI usage:

### Postgres user authorization 

```shell script
schema permissions apply tests/files/gebieden_auth.json tests/files/profiles/gebieden_test.json level_b_user LEVEL/B
```
Here, `tests/files/gebieden_auth.json` is a local file containing an example Amsterdam Schema, 
`tests/files/profiles/gebieden_test.json` is a local file containing an example profile.
`level_b_user` is an existing postgres role, which will be granted read priviliges in accordance with scope `LEVEL/B`.

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









