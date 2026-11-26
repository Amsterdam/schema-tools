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

The decision is to encapsulate schema-domain logic in the domain objects, with
`DatasetGraph` as the root aggregate. `Dataset` owns dataset-local concepts;
`DatasetGraph` owns the complete resolved graph and the shared identities of its
objects. Objects express schema meaning and behavior through their own state and
direct relationships, rather than relying on consumers to reimplement that
logic or asking the graph to resolve relationships at runtime.

`amsterdam-schema` remains the authority for authored definitions and their
validation contracts. `schema-tools` materializes those definitions and their
dependencies into the domain graph. Persisted metadata and generated models are
projections for consumers; they do not define domain behavior or become
independent sources of schema meaning.

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

The domain describes what the schema means—its identity, relationships,
structure, and rules. Consumers decide how to store, name, serialize, present,
or execute based on that information. A helper belongs in the domain when it
expresses that meaning, not merely because it is convenient or purely computational.

## Consequences

- Consumers receive a ready-to-use graph, rather than triggering dependency
  loading through individual objects. Repository adapters assemble the graph;
  domain objects encapsulate the behavior consumers need from it.
- Persisted metadata and generated models remain projections that can be traced
  back to the authored definitions.
- The refactor preserves domain behavior, not every detail of the legacy object
  interface. Representation-specific helpers move to consumers instead of
  returning to domain objects.

This ADR sets ownership and guiding principles; it does not prescribe concrete
module interfaces or the refactor is complete. Operational configuration
unrelated to schema meaning is out of scope.
