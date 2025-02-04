# 2025-01-31 (6.2.1)

* Auth fields can now accomodate references to separately defined scope
  objects.

# 2025-01-29 (6.2)

* Added a `validate_scopes` command to use in the Amsterdam Schema
  pre-commit.

# 2025-01-29 (6.1.4)

* Added `enable_export` column to the `datasetTable` model to be able
  to configure the exports per table.

# 2025-01-16 (6.1.3)

* Fixed `create_mock_data` to accept valid Amsterdam Schema
* Updated the arguments for the `create_mock_data`, `relate_mock_data` and `truncate_tables` to accept the
  DATASETS_LIST and DATASETS_EXCLUDE env vars.

# 2024-10-12 (6.1.2)

* Added pool_pre_ping=True to fix connection pool issues.

# 2024-10-01 (6.1.1)

* Fixed crash in `schema permissions apply` for tables that don't have a sequence in the database.

# 2024-10-01 (6.1)

* Added SQL SEQUENCE permissions in `schema permissions apply` to make `pg_dump` access easier.
* Added `has_field_filter_access()` and `filter_auth` (`filterAuth` in schema) to add permissions for list filtering.
* Added `DatasetFieldSchema.is_relation` for convenience.
* Fixed `schema permissions apply` to set the correct database grants for nested/through tables (only affects `brk.kadastraleobjecten.soortCultuurBebouwd` for now).
* Fixed accessing `DatasetFieldSchema.unit` if the unit is missing.
* Fixed typo in unused property `discription_with_unit` -> `description_with_unit`.
* Fixed clearing cached properties when `auth` or `filterAuth` are updated.

# 2024-07-15 (6.0.1)

* Fix Django<4.2 pinning.

# 2024-07-11 (6.0)

* Improved performance of auth checks (especially `has_field_access()`).
* Changed method signature of `UserScopes.has_all_scopes()` and `UserScopes.has_any_scopes()`
  (most callers should use `has_field_access()` anyway.)
* Block `deepcopy()` of schema fields, as it's very slow.
* Removed `DatasetType` base class.
* Removed `schema import events` code, as its no longer used.
* Removed deprecated API's (`schematools.utils` and `is_relation_temporal`).
* Removed wirerope dependency.
* Removed Python 3.8 style annotations.

# 2024-03-04 (5.27.0)

* Add `unit` and `description_with_unit` properties to fields.

# 2024-03-04 (5.26.1)

* Change `id_field` to snake_case. It is directly used in sql queries.

# 2024-02-29 (5.26.0)

* Added extra `id_field` to Datasettable. Needs to be configurable for geosearch.

# 2024-01-07 (5.25.0)

* Added temporal indexes

# 2024-01-06 (5.24.1)

* Fix bug in exporter where a loop through dataset tables was prematurely
  left when a tables has not export data.

# 2024-01-06 (5.24.0)

* Do not write an empty export file if no columns are selected.
  Also fix the export to only use active records for jsonlines.

# 2024-01-31 (5.23.4)

* Bugfix in _is_valid_sql

# 2024-01-25 (5.23.3)

* Fix in _is_valid_sql to fix materialized view problem.

# 2024-01-23 (5.23.2)

* Fix to the _get_scopes to return the correct scopes for both dataset, table
  and table fields.

# 2024-01-22 (5.23.1)

* Fix the storage of datasettables.display_field
  (an old copy/paste error in the codebase)

# 2024-01-12 (5.23.0)

* Modified create_views functions to support materialized views

# 2024-01-10 (5.22.0)

* Add `enable_export` column to the `dataset` model to be able
  to configure the exports per dataset.

# 2024-01-08 (5.21.2)

* Remove the Django >= 4.2 pinning, because DSO is still on Django 3.x.
  Later on, we can migrate both schematools and DSO simultaneous to >= 4.2

# 2023-12-28 (5.21.1)

* Fix auth property for subfields. The subfields do not have
  scopes, however, a scope can be defined on the parent field.

# 2023-12-20 (5.21.0)

* Added an extra helper method to user-scopes to determine
  if one of the fields has a scope that is blocking access.

# 2023-12-15 (5.20.1)

* Some old definitions (gebieden.stadsdelen) are using temporal relations
  defined as a plain string instead of an objects. The exporter
  need to take this into account.

# 2023-12-15 (5.20.0)

* Change export to only use active records for csv and jsonlines,
  so, no historical records. Also brought the export more in line
  with the csv export of the DSO-API:
  - headers using capitalize()
  - date-time in iso notation
  - foreign keys only with an `identificatie` (no `volgnummer`)

# 2023-12-05 (5.19.1)

* Updated Django version > 4.2
* Updated the github workflow to use postgres14 image

# 2023-12-01 (5.19.0)

* Improve possibility to use git commit hashes when creating SQL migrations
  from amsterdam schema table definitions. Now also supports schemas
  with table definitions in separate files.

# 2023-12-01 (5.18.0)

* Add possibility to use git commit hashes when creating SQL migrations
  from amsterdam schema table definitions.

# 2023-11-24 (5.17.18)

* Bugfix: Update nested table when nested field name has underscore.
* Bugfix: Update parent table when parent table has shortname for update events.
* Bugfix: Only check for row existence when table exists.

# 2023-10-18 (5.17.17)

* Bugfix: Ignore id when copying data from temp table to main table for nested tables.

# 2023-10-18 (5.17.16)

* Bugfix: Snake case temp table schema name in EventProcessor.

# 2023-10-18 (5.17.15)

* Bugfix: Don't try to create schema if schema already exists. Fails on 'create schema'
  permissions.

# 2023-10-18 (5.17.14)

* Bugfix: Fixed issue where duplicate indexes were created

# 2023-10-06 (5.17.13)

* Bugfix: Cache nested tables in EventProcessor.

# 2023-10-06 (5.17.12)

* Bugfix: Reset last eventid after a manually aborted full load sequence.

# 2023-10-05 (5.17.11)

* Bugfix: Fix full event loads for relation tables referencing tables with shortname.

# 2023-10-05 (5.17.10)

* Bugfix: Fixed view_data insertion into datasets.dataset

# 2023-10-05 (5.17.9)

* Bugfix: Fixed case where nested table has a parent table that uses shortname.

# 2023-10-04 (5.17.8)

* Bugfix: Fixed bug in _is_valid_sql.
* Bugfix: Assigned create and usage rights to write_user for creating views.

# 2023-09-26 (5.17.7)

* Bugfix: Fix error in permissions script, introduced a `view_owner`
  role that owns all views.

# 2023-09-25 (5.17.6)

* Bugfix: Fix error when nested object in event is null.

# 2023-09-25 (5.17.5)

* Bugfix: Fix error when relation table is not present during a relation full load.
* Bugfix: Fix error when trying to update relation from None value.

# 2023-09-23 (5.17.4)

* Bugfix: update nested tables in EventProcessor.

# 2023-09-21 (5.17.3)

* Bugfix: check for required permissions was not taking the `OPENBAAR`
  scope into account in the correct way.

# 2023-09-16 (5.17.2)

* Fix: Cast datetime type to string, because of a out-of-range year in bag_panden.

# 2023-09-16 (5.17.1)

* Bugfix: Fix error when invalid table is entered in derivedFrom paramter
* Bugfix: Fixed error in detecting if write user exists

# 2023-09-14 (5.17.0)

* Feature: Added create-views command to django management commands to facilitate creating views.

# 2023-09-14 (5.16.1)

* Bugfix: Ignore empty input lines in NDJSONImporter.

# 2023-09-07 (5.16.0)

* Feature: Use dataset-specific schema to store temporary full load tables.
* Bugfix: Update main table relations after full load of relation table.

# 2023-09-07 (5.15.1)

* Bugfix: Fix case of updating parent table where two relations exist where the name of one
  relation is a prefix of the other relation.

# 2023-09-06 (5.15.0)

* Feature: Added the option `--additional-grants` to the
  `schema permissions apply` script to be able to set grants
  for non-amsterdam-schema tables. This is needed for the `datasets_*` tables,
  because on Azure these tables are accessed in PostgreSQL from a user
  (or the anonymous) account and the `scope_openbaar` scopt has to be granted for
  these tables.

# 2023-09-05 (5.14.2)

* Bugfix: For the edge case that the dataset has the id `datasets`
  the validator was not behaving correctly. That has now been fixed.

# 2023-08-30 (5.14.1)

* Bugfix: Fix missing fields in through table (second try).

# 2023-08-28 (5.14.0)

* Feature: EventProcessor: Process events for which no relation table exists, does update parent table.

# 2023-08-22 (5.13.4)

* Bugfix: Fix missing fields in through table.
  If a relation has extra properties defined on the relation,
  these properties should also be available on the through table that is
  created for this relation.

# 2023-08-16 (5.13.3)

* Bugfix: Altered UnlimitedCharField to not throw an exception when max_length is found in kwargs

# 2023-07-24 (5.13.2)

* Bugfix: nullable_int faker did not play well with enums, is now fixed.
* Added cli option to mocker to limit the tables.

# 2023-07-13 (5.13.1)

* Feature: EventProcessor: Track processed event ids now for full load sequences as well.

# 2023-07-13 (5.13.0)

* Feature: EventProcessor: Track processed event ids to avoid duplicate processing and key collisions.

# 2023-06-30 (5.12.5)

* Bugfix: Fix constructing id's for tables where the id keys contain underscores.

# 2023-06-21 (5.12.4)

* Bugfix: Removed a check for datasets with status beschikbaar in schematools/permissions/db.py set_dataset_read_permissions.
* Bigfix: Changed tests/test_export.py test_jsonlines_export to account for percision differences

# 2023-06-09 (5.12.3)

* Bugfix: Use engine.connect() instead of engine.execute() directly. Not supported anymore in SQLAlchemy 1.4.
* Bugfix: Use column names in INSERT INTO statement instead of column positions.

# 2023-06-08 (5.12.2)

* Fix bug in event processor. Use shortname attribute when updating parent table.

# 2023-06-08 (5.12.1)

* Fix bug in event processor. Don't try to update parent tables for relation tables of n:m relations.

# 2023-06-07 (5.12.0)

* Implement logic to recover from failed event messages

# 2023-06-05 (5.11.6)

* Two small fixes to make `sqlmigrate_schema` work:
  - requires_system_checks needs to be a list (from Django 1.4)
  - list of datsets need to be a set when calling Django schema migrate API

# 2023-05-24 (5.11.5)

* Patch to fix custom implementation of UnlimitedCharField.max_length

# 2023-05-24 (5.11.4)

* Recognize more than 2 consecutive capital letters as word boundaries
* Fix database column naming in model mocker class construction

# 2023-05-24 (5.11.3)

* Fix handling of geometry fields containing underscores in the attribute name.
* Add utility cli commands for case-changes (snake, camel).

# 2023-05-23 (5.11.2)

* Make export to csv/jsonlines less memory hungry.

# 2023-05-17 (5.11.1)

* Add serialization of Decimal for orjson.dump() in exporter.

# 2023-05-16 (5.11.0)

* Add option ``ind_create_pk_lookup`` to ``EventsProcessor``, to skip
  expensive index creation.

# 2023-05-10 (5.10.2)

* Add UUID column type for introspection of PostgreSQL db.

# 2023-05-08 (5.10.1)

* Add a `--to-snake-case` option to the `schema show dataset[table]` cli functions.

# 2023-05-04 (5.10.0)

* Add support for loading events in batches.
  Extract initialisation and finalisation into separate methods to improve performance.
  Cache initialised tables.

# 2023-04-20 (5.9.3)

* Disable the versioning that creates postgresql schemas for new tables.
  This functionality is not fully completed and accepted and is now
  blocking the event processing code.

# 2023-04-13 (5.9.2)

* Skip index creation on temporary full load table from event importer.
* Fix truncate bug that truncated all associated tables when updating a relation table.

# 2023-04-07 (5.9.1)

* Add support for `first_` and `last_of_sequence` headers for event importer.

# 2023-04-06 (5.9.0)

* Simplification of the events importer. Relations are now imported as separate objects.

# 2023-04-05 (5.8.6)

* Apply some small fixes to cli commands and update template used to generate schema by introspection.

# 2023-04-04 (5.8.5)

* Exclude all array-type fields during exports.

# 2023-04-03 (5.8.4)

* Add cli commands to list schemas and tables.

# 2023-03-30 (5.8.3)

* Workaround for DSO-API docs not loading.

# 2023-03-28 (5.8.2)

* Fix condition for through tables for a 1-N relation.

# 2023-03-22 (5.8.1)

* Pin SQLAlchemy to >= 1.4, < 2.0 to make schematools usable
  from Airflow 2.4.1.

# 2023-03-22 (5.8.0)

* Add export cli commands to export geopackages, csv and jsonlines.

# 2023-03-20 (5.7.0)

* Through tables for a 1-N relation is now based on the fact that
  the object field definition in the schema has additional attributes
  that are not part of the relation key.

# 2023-03-08 (5.6.12)

* Security fix: authorisation on fields with subfields was incorrectly
  handled.

# 2023-02-27 (5.6.11)

* The ``schema validate`` command was fixed to work with v2 publishers.
* Validation errors are reporting in a hopefully more readable format.
* ``enum`` values in schemas are now type-checked during validation.

# 2023-02-22 (5.6.10)

* Require SQLAlchemy <= 1.12.5

# 2023-02-21 (5.6.9)

* Fix structural validation of publisher references by not inlining them in the json held against the metaschema.

# 2023-02-14 (5.6.8)

* Pin pg-grant to 0.3.2 to stay compatible with SQLAlchemy

# 2023-02-07 (5.6.7)

* Bugfix Dataset.json not properly dereferencing publisher property

# 2023-02-07 (5.6.6)

* Fix names for the subfields of an objectfield. These names need a prefix,
  because they are exposed externally in the DSO API.

# 2023-02-01 (5.6.5)

* Print error path as is from batch-validate.
* Bugfix for loader methods ``get_publisher`` and ``get_all_publishers``.
* Dataset.publisher returns publisher object irrespective of schema version.

# 2023-01-30 (5.6.4)

* Add whitelist to exclude certain datasets from the path-id validator.

# 2023-01-30 (5.6.3)

* Pin SQLAlchemy to a version smaller than 1.4.0, because `pg_grant` breaks on
  a higher version.

# 2023-01-25 (5.6.2)

* Bugfix for for name clashes that occur in Django ORM relation fields
  when two versions of the same dataset are deployed next to eachother.

# 2023-01-24 (5.6.1)

* Bugfix for regression which caused dataset id to be matched with the path of a table
when the validated schemafile is a table.

# 2023-01-23 (5.6.0)

* Feature added to enable use of object fields in amsterdam schema.
  Those fields are flattened in the relational schema (added to the parent table).
  Furthermore, a second type of object field with `"format": "json"` has been added.
  For those fields an opaque json blob will be added in the relational database.

# 2023-01-17 (5.5.2)

* Correctly resolve the publisher URL, regardless of whether there is a trailing slash

# 2023-01-16 (5.5.1)

* ``schema batch-validate`` now produces more readable error messages.

# 2023-01-13 (5.5.0)

* Bugfix in CLI batch_validate that caused validation to stop at the first invalid schema
* Bugfix in CLI batch_validate that caused dataset.json files in nested directories to be unresolvable

SUPPORTED METASCHEMAS: 1 2

# 2023-01-10 (5.4.0)

* The `schema ckan` command was changed to generate unique (we hope) titles
* Bugfix for getting pubishers from an online index
* Bugfix in publisher validation logging

SUPPORTED METASCHEMAS: 1 2

# 2022-12-21 (5.3.0)

* Bugfix in batch_validate that treats extra_meta_schema_url as an argument instead of an option.
* Add pre-commit hook for validating publishers

SUPPORTED METASCHEMAS: 1 2

Note that support is not guaranteed yet, for now this a declaration of intention. Any bugs should be reported.

# 2022-12-20 (5.2.0)

* Support loading and validating publishers from the schema-server.
* Make schematools aware of the metaschema major versions it can work with.
* Support for attempting validation against multiple metaschemas.

SUPPORTED METASCHEMAS: 1 2

# 2022-12-19 (5.1.6)

* Several minor fixes to tests.
* Removal of unused DatasetSchema.identifier property
* Add `neuronId` is mapping needed for through table identifiers

# 2022-12-14 (5.1.5)

* Mocked schemas now use properly camel-cased field names.
* Relations can be primary keys.
* The command `schema batch-validate` now works on table files as well as
  `dataset.json` files.

# 2022-12-13 (5.1.4)

* Fix importing schema files by using a relative path.
* Fix `related_dataset_schema_ids` to also detect changes in nested objects.
* Fix `DatasetTableSchema.get_fields()` to return cached instances too.
* Fix `verbose_name` of `GeometryField` in Django ORM, which reused globally defined data.
* Fix performance of iterating over subfields, no longer needs to load related tables.
* Added `DatasetFieldSchema.is_nested_object` property.
* Normalized exceptions for missing datasets/tables/fields:
  * The `DatasetNotFound` exception extends from `SchemaObjectNotFound`.
  * Added `DatasetTableNotFound` and `DatasetFieldNotFound`.
  * There is no need for `except (DatasetNotFound, SchemaObjectNotFound)` code, it can all be `except SchemaObjectNotFound:`.
* Cleanup Django model field creation logic.
* Cleanup SQLAlchemy column creation logic.
* The schema validator now rejects tables with both an 'id' field and a composite primary key.


# 2022-12-01 (5.1.3)

* Fix `limit_tables_to` issue with crash in index creation for skipped tables.
* Fix `limit_tables_to` issue for M2M relations, now reports the table is not available.
* Fix SRID value for SQLAlchemy geometry columns (were always RD/NEW).
* Fix CKAN upload to skip datasets that are marked as "not available".
* Improved 3D coordinate system detection, and added more common SRID values.
* Improved naming of geometry column index to be consistent with other generated indices.


# 2022-11-24 (5.1.2)

* Fix `BaseImporter.generate_db_objects()` to handle properly snake-cased table identifiers values for table creation.
* Improve the underlying `tables_factory()` logic to support snake-cased table identifiers for all remaining parameters.


# 2022-11-22 (5.1.1)

* Improve `limit_tables_to` to accept snake-cased table identifiers, which broke Airflow jobs.
  This addresses an inconsistency between parameters, where `BaseImporter.generate_db_objects()`
  allowed snake-cased identifiers for `table_id`, but needed exact-cased values for `limit_tables_to`.


# 2022-11-21 (5.1)

A big change in schema loading.

This mostly affects unit tests in other projects, or files that do custom schema loading.
Unit test code should preferably use a `schema_loader` instance per test run,
as all datasets are only cached within the same loader instance now.

* Added `schematools.loaders.get_schema_loader()` that provides a single object instance for loading.
* Added `DatasetSchema.table_versions` mapping to access other table versions by name.
* Added `Record.source` attribute to `BaseImporter.load_file()` and `parse_records()` return values.
  This allows callers to inspect the source record, e.g. for cursor handling.
* Removed `TableVersions` injection in dataset schema data. Tables are now loaded on demand.
* Removed internal global dataset cache, datasets are only cached per loader.
* Removed ununsed functions in `schematools.utils`.
* Deprecated loading functions in `schematools.utils`, use `schematools.loaders` instead.


# 2022-11-15 (5.0.2)

* Using `BigAutoField` for all identifier fields now by default.
* Fixed Django system check warnings for `AutoField`/`BigAutoField` migration changes.
* Fixed CKAN metadata upload to https://data.overheid.nl/ for datasets without a description or title.


# 2022-11-02 (5.0.1)

* Added validation check to prevent field names from being prefixed with their table or dataset name.
* Fixed Django ``db_column`` for subfields that use a shortname (regression by 5.0).
* Fixed dependency pinning of shapely to 1.8.0


# 2022-10-31 (5.0)

A major new release that cleans up various internal API's.

* Added many improvements to creating mock data.
* Changed CLI arguments for mocking to be more intuitive.
* Changed schema loaders to return relative paths instead of dataset ID's.
* Changed test runner to skipping tests that require the database.
* Completely rewrote the NDJSON importer for simplicity.
* Completely rewrote database index creation for simplicity.
* Fixed shortname leaking via ``Dataset{Table,Field}Schema.name`` attributes (also see PR #332 and #344).
* Fixed display/geometry field notation as exposed via ``dataset_field`` table.
* Fixed importing datasets from the filesystem that are namespaced inside a subfolder.
* Fixed using schemaloader in Django management commands.
* Fixed ``saloger`` fixture leaking to every other test, flooding the console.
* New API's:

  * ``DatasetSchema``:

    * ``python_name`` (formats as ClassName)
    * ``db_name`` (formats in snake\_case)

  * ``DatasetTableSchema``:

    * ``python_name`` (formats as ClassName)
    * ``short_name``
    * ``through_fields`` (for through tables)
    * ``temporal.identifier_field``
    * ``main_geometry_field``
    * ``identifier_fields``

  * ``DatasetFieldSchema``:

    * ``python_name``
    * ``is_identifier_part``
    * ``is_subfield``
    * ``srid``
    * ``related_fields``
    * ``nested_table``
    * ``through_table``

* Changed API's:

  * ``DatasetTableSchema``:

    * ``display_field`` returns actual field now.
    * ``temporal.dimensions`` returns actual fields now.
    * ``db_name()`` => ``db_name``   became a property for the typical common usage.
    * ``db_name_variant()``          provides the versioned-table support

  * ``DatasetFieldSchema``:

    * ``db_name()`` => ``db_name``  - became a property for consistency
    * ``is_temporal`` => ``is_temporal_range``
    * ``get_subfields()`` => ``subfields``   - no longer needs prefixes.

* Moved ``to_snake_case()`` / ``toCamelCase()`` imports to ``schematools.naming``
* Deleted obsolete / unused functions:

  * ``DatasetTableSchema.name`` (use the ``id``, ``db_name``, or ``python_name`` instead).
  * ``get_dimension_fieldnames()``
  * ``get_through_tables_by_id()``
  * ``get_fields_by_id()``
  * ``shorten_name()``
  * ``_get_fk_fields()``

* Removed ``DatasetTableSchema.get_subfields(add_prefixes=True)`` logic as the new naming attributes address that.
* Removed unused Docker stuff in ``consumer/`` folder.
* Removed ``more-itertools`` dependency.
