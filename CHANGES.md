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
