# 2023-02-22(5.6.10)
* Require SqlAlchemy <= 1.12.5
# 2023-02-21(5.6.9)

* Fix structural validation of publisher references by not inlining them in the json held against the metaschema.

# 2023-02-14(5.6.8)

* Pin pg-grant to 0.3.2 to stay compatible with SQLAlchemy

# 2023-02-07(5.6.7)

* Bugfix Dataset.json not properly dereferencing publisher property

# 2023-02-07(5.6.6)

* Fix names for the subfields of an objectfield. These names need a prefix,
  because they are exposed externally in the DSO API.

# 2023-02-01(5.6.5)

* Print error path as is from batch-validate.
* Bugfix for loader methods get_publisher and get_all_publishers.
* Dataset.publisher returns publisher object irrespective of schema version.

# 2023-01-30(5.6.4)

* Add whitelist to exclude certain datasets from the path-id validator.

# 2023-01-30(5.6.3)

* Pin SQLAlchemy to a version smaller than 1.4.0, because `pg_grant` breaks on
  a higher version.

# 2023-01-25(5.6.2)

* Bugfix for for name clashes that occur in Django ORM relation fields
  when two versions of the same dataset are deployed next to eachother.

# 2023-01-24 23(5.6.1)

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
