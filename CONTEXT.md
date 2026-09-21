# CONTEXT

## Purpose

schema-tools is a Python library and CLI for working with Amsterdam Schema definitions and their lifecycle in relational databases.

It supports:

- Reading dataset schemas from files.
- Creating SQL database objects from schema definitions.
- Importing data into those objects.
- Exporting data from database tables into interchange formats.
- Validating schemas and relations.
- Generating integration artifacts for selected consumers (for example Django and Databricks).

## Core Concepts

### Dataset

A dataset is the top-level schema object loaded from a dataset.json file.

A dataset contains versions and tables.

### Version

A version selects a concrete set of table definitions for a dataset.

Exports and table loading are always evaluated against a chosen version.

### Table

A table is a logical entity in a dataset version.

A table has fields, optional temporal behavior, optional geometry, scopes, and export configuration.

### Field

A field is a typed attribute of a table.

A field can be:

- Scalar
- Nested
- Array
- Geometry
- Relation to another table (same dataset or another dataset)

### Relation

A relation references another dataset:table target and is resolved through the schema loader.

Relations can provide keys for joins and can be used as mainGeometry references.

### Export Definition

An export definition selects:

- filetype
- scopes
- table set
- naming

It drives which tables are exported and how outputs are grouped.

### Scope

Scopes are access labels applied on dataset/table/field level.

An exporter only includes fields whose effective scopes are allowed by the export definition.

### Main Geometry

A table may declare mainGeometry.

mainGeometry can point to:

- a geometry field on the same table
- a relation field that resolves to another table containing the geometry

## Runtime Components

### Loader Layer

Loads and caches dataset schemas from filesystem sources.

Must be able to resolve related datasets when cross-dataset relations are used.

### Type Model

Python objects in schematools.types represent dataset, version, table, and field semantics.

Business rules live here (for example relation resolution and derived naming).

### Factories Layer

Builds SQLAlchemy Table objects from schema model definitions.

Naming and schema defaults are centralized here.

### Importers

Convert source payloads (for example NDJSON, events) into DB rows according to schema model.

### Exporters

Read DB rows and produce output files (csv, geojson, geopackage, jsonlines).

Geometry-aware exporters perform CRS transformations where needed.

### Validation

Checks schema consistency, required fields, relation correctness, and other invariants.

## Invariants

- Dataset and table IDs must resolve consistently through the configured loader.
- SQL table naming must be deterministic from schema model and version.
- Export scope filtering must be deterministic and reproducible.
- Cross-dataset relations are valid only when the related dataset is discoverable by the same loader root.

## Common Failure Modes

- Missing related dataset in loader root causes relation resolution errors.
- Export definitions that omit required geometry context can yield empty geojson features.
- Mismatch between schema version and available DB tables causes runtime export/import failures.
- Incorrect join construction for relation-based geometry can cause cartesian products or dropped rows.

## Working Agreement

When adding features or fixes, prefer:

- Extending schema model behavior in one place rather than per-command duplication.
- Keeping exporter query construction explicit (selected columns, from source, join condition).
- Adding tests for both same-dataset and cross-dataset relation cases when relation logic changes.
