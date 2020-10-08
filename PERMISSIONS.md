# Permissions at the Database Level

Authorization is set in two different ways:
- Amsterdam Schema Authorization.  An "auth" field may be set at dataset, tabel, or field level.
- Profile Authorization.

This document describes how the CLI can be used to set permissions at the database level using these two methods.



## CLI

Examples of CLI usage:

### Setting permissions using Dataset Level Authorization in Amsterdam Schema

```
schema permissions from_schema tests/files/gebieden.json myrole BRP/R
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
Postgres role `myrole` is required to exist. The command reads the Amsterdam Schema file, checks for the presence
of Dataset Level Authorization, and if present, will check if the scope matches, and if so, 
will give read permission to myrole for all tables associated with the dataset.



### Setting permissions with a Profile
```
schema permissions create tests/files/profiles/gebieden_test.json
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

The `permissions create` command will read the Profile, iterate over all scopes and datasets, and set the appropriate permissions in all tables belonging to those datasets.

Current limitations:
- only `read` permissions
- only Dataset Details Structure supported (see AUTH_PROFILES.md). In progress!

### Introspection of permissions

```shell script
schema permissions introspect
```
Lists all ACLs from database (`pg_grant` format)

### Setting permissions with Amsterdam Schema Authorization
TODO


## Tests

Tests may be run from the schema-tools root directory:
```shell
pytest tests/test_permissions.py
```









