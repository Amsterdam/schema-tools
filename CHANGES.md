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
