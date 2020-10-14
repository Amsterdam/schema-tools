# Permissions at the Database Level

Authorization is set in two different ways:
- Amsterdam Schema Authorization.  An "auth" field may be set at dataset, tabel, or field level.
- Profile Authorization.

This document describes how the CLI can be used to set permissions at the database level using these two methods.



## CLI

Examples of CLI usage:

### Schema Authorization in Amsterdam Schema - single dataset schema

```
schema permissions from_schema tests/files/gebieden.json brp_r BRP/R
```
Here, tests/files/gebieden.json is a local file containing an example Amsterdam Schema. 
Dataset Level Authorization is used to restrict acccess to scope "BRP/R": 
```json
{
  "type": "dataset",
  "id": "gebieden",
  "title": "gebieden",
  "auth": "BRP/R",
    ...
```
Postgres role `brp_r` is required to exist for this example (`CREATE ROLE brp_r` first if necessary). 
The command reads the Amsterdam Schema file, checks for the presence
of Dataset Level Authorization or Table Level Authorization, and if present, will check if the scope matches, and if so, 
will give read permission to myrole for all associated tables.

### Schema Authorization in Amsterdam Schema - complete schema

```
schema permissions from_schema_url brp_r BRP/R
```
Here, the environment variable `SCHEMA_URL` is used to download a complete schema collection, 
and grants read permission to brp_r for all tables with auth scope `BRP/R`.

### Profile Authorization
```
schema permissions from_profile tests/files/profiles/gebieden_test.json myrole FP/MD
```

Here, `tests/files/profiles/gebieden_test.json` is a local file containing an example Profile:
```json
{
  "name": "gebieden_test",
  "scopes": [
    "FP/MD"
    ],
  "schema_data": {
    "datasets": {
      "gebieden": {
        "permissions": "read"
      }
    }
  }
}
```
It states that the scope `FP/MD` should have read access to all tables belonging to dataset `gebieden`.
Within postgres, these are all tables starting with `gebieden_`.

Instead of a local file path you may also specify the location of the profile with a web accessible URL

The `permissions from_profile` command will read the Profile, iterate over all scopes and datasets, 
and set the appropriate permissions in all tables belonging to those datasets.

Current limitations:
- only `read` permissions
- only Dataset Details Structure supported (see AUTH_PROFILES.md). In progress!

### Introspection of permissions

```shell script
schema permissions introspect brp_r
```
Lists all table permissions for postgres role brp_r

### Revoking permissions
```
schema permissions revoke brp_r
```
Revokes all table permissions for postgres role brp_r

## Tests

Tests may be run from the schema-tools root directory:
```shell
pytest tests/test_permissions.py
```









