# ADR-0001: Schema authority and aggregate boundary

- **Status:** Draft
- **Date:** 2026-11-05
- **Applies to:** `amsterdam-schema`, `schema-tools` (this), and `dso-api`

## Context

Schema information takes several forms: authored definitions, runtime objects,
persisted metadata, and generated models. When it's unclear which form owns
which decisions, they can drift apart or mix schema meaning with storage and
presentation details.

Because datasets can refer to one another, a single dataset definition is not
always enough for runtime use.

## Decision

`amsterdam-schema` is the authority for schema definitions and the structural
rules used to validate them. `schema-tools` contains further semantic validation
`schema-tools` also turns those definitions into the runtime graph. Persisted
metadata and generated models are useful projections of that graph, but they do
not belong in the domain model.

The graph includes the full set of datasets needed to follow their relationships.
Each `Dataset` owns its own concepts, while `DatasetGraph` holds the resolved
collection and the shared objects those relationships point to. Consumers can
look up an object in the graph, but objects navigate through their existing
references; they do not use the overarching graph to look up the objects they
relate to.

Repository adapters load and prepare the graph before making it available to
consumers. They resolve references, normalize values, and finish wiring
relationships — including cycles — during that private assembly. Once materialized,
objects are ready to use: a missing optional relationship means it truly does
not apply, not that assembly is unfinished. Navigation does not perform I/O or
walk back up the object tree to recover context.

This preparation is not another validation pass. Schema definitions are
validated before they are merged into `amsterdam-schema`; materialization then
builds the runtime representation without repeating schema or graph-wide
validation. Adapters must report retrieval and resolution failures clearly, and
their tests must establish that the graph is assembled correctly.

Finally, the domain describes what the schema means—its identity, relationships,
structure, and rules. The modules that use it decide how to store, name,
serialize, present, or execute that information. A helper does not belong in
the domain just because it is convenient or purely computational; its
responsibility matters.

## Consequences

- Consumers receive a ready-to-use graph, rather than triggering dependency
  loading through individual objects. The work of assembling that graph stays
  in repositories.
- Persisted metadata and generated models remain projections that can be traced
  back to the authored definitions.
- The refactor preserves domain behavior, not every detail of the legacy object
  interface. Representation-specific helpers move to consumers instead of
  returning to domain objects.

This ADR sets ownership and guiding principles; it does not prescribe concrete
module interfaces or the refactor is complete. Operational configuration
unrelated to schema meaning is out of scope.
