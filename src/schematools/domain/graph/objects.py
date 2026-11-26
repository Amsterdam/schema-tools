from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date
from enum import Enum
from types import MappingProxyType
from typing import Any, ClassVar, Literal, NamedTuple, Protocol, TypeVar

from schematools.exceptions import (
    DatasetFieldNotFound,
    DatasetNotFound,
    DatasetTableNotFound,
    DatasetVersionNotFound,
)
from schematools.types import Faker, Permission, PermissionLevel, SemVer


@dataclass(slots=True, eq=False)
class Field:
    """A field node whose references are wired during graph materialization."""

    id: str
    relation: TableRelation | None = None
    scopes: frozenset[Scope] = frozenset()
    table: Table = dataclass_field(kw_only=True, repr=False)
    parent_field: Field | None = dataclass_field(default=None, kw_only=True, repr=False)
    type: str = dataclass_field(default="string", kw_only=True)
    title: str | None = dataclass_field(default=None, kw_only=True)
    description: str | None = dataclass_field(default=None, kw_only=True)
    unit: str | None = dataclass_field(default=None, kw_only=True)
    shortname_override: str | None = dataclass_field(default=None, kw_only=True)
    required: bool = dataclass_field(default=False, kw_only=True)
    format: str | None = dataclass_field(default=None, kw_only=True)
    multipleof: float | None = dataclass_field(default=None, kw_only=True)
    faker: Faker | None = dataclass_field(default=None, kw_only=True)
    provenance: str | None = dataclass_field(default=None, kw_only=True)
    crs_override: str | None = dataclass_field(default=None, kw_only=True)
    subfields: tuple[Field, ...] = dataclass_field(default=(), kw_only=True)
    item_type: str | None = dataclass_field(default=None, kw_only=True)
    item_definition: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)
    constraints: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)
    filter_scopes: frozenset[Scope] = dataclass_field(default=frozenset(), kw_only=True)
    nested_table: Table | None = dataclass_field(default=None, kw_only=True, repr=False)

    @property
    def shortname(self) -> str:
        return self.shortname_override if self.shortname_override is not None else self.id

    @property
    def is_identifier_part(self) -> bool:
        return not self.is_subfield and self in self.table.identifier_fields

    @property
    def is_relation(self) -> bool:
        return self.relation is not None

    @property
    def nm_relation(self) -> TableRelation | None:
        if self.relation is not None and self.relation.kind is RelationKind.MANY_TO_MANY:
            return self.relation
        return None

    @property
    def related_table(self) -> Table | None:
        return self.relation.target if self.relation is not None else None

    @property
    def related_fields(self) -> tuple[Field, ...] | None:
        if self.relation is None:
            return None
        if self.relation.target_fields is not None:
            return self.relation.target_fields
        fields = self.relation.target.identifier_fields
        return fields[:1] if self.is_loose_relation else fields

    @property
    def related_field_ids(self) -> list[str] | None:
        fields = self.related_fields
        return [field.id for field in fields] if fields is not None else None

    @property
    def reverse_relation(self) -> AdditionalRelation | None:
        return self.relation.reverse if self.relation is not None else None

    @property
    def is_object(self) -> bool:
        return self.type == "object"

    @property
    def is_scalar(self) -> bool:
        return self.type not in {"object", "array"}

    @property
    def is_subfield(self) -> bool:
        return self.parent_field is not None

    @property
    def is_temporal_range(self) -> bool:
        if self.parent_field is None or self.format not in {"date", "date-time"}:
            return False
        target = self.parent_field.related_table
        return (
            target is not None
            and target.temporal is not None
            and any(field.id == self.id for field in target.temporal.temporal_fields)
        )

    @property
    def is_geo(self) -> bool:
        return "geojson.org" in self.type

    @property
    def crs(self) -> str | None:
        if not self.is_geo:
            return None
        return self.crs_override or self.table.crs

    @property
    def srid(self) -> int | None:
        crs = self.crs
        if crs is None:
            return None
        if not crs.startswith("EPSG:"):
            raise ValueError(f"Expected EPSG:<integer> CRS, got {crs!r}.")
        return int(crs.removeprefix("EPSG:"))

    @property
    def is_array(self) -> bool:
        return self.type == "array"

    @property
    def is_array_of_objects(self) -> bool:
        return self.is_array and self.item_type == "object"

    @property
    def is_array_of_scalars(self) -> bool:
        return self.is_array and self.item_type != "object"

    @property
    def is_nested_object(self) -> bool:
        return self.is_object and self.relation is None

    @property
    def through_table(self) -> Table | None:
        return self.relation.through_table if self.relation is not None else None

    @property
    def relation_attributes(self) -> set[str]:
        target = self.related_table
        return {field.id for field in self.subfields} - set(target.identifier) if target else set()

    @property
    def is_composite_key(self) -> bool:
        target = self.related_table
        return self.is_object and target is not None and target.has_composite_key

    @property
    def is_loose_relation(self) -> bool:
        return self.relation is not None and self.relation.loose

    def get_field_by_id(self, field_id: str) -> Field:
        for field in self.subfields:
            if field.id == field_id:
                return field
        raise DatasetFieldNotFound(f"Subfield {field_id!r} does not exist in {self.id!r}.")


class TableKind(Enum):
    REGULAR = "Regular"
    THROUGH = "Through"
    NESTED = "Nested"


class TableStatus(Enum):
    stable = "stable"
    under_development = "under_development"
    beschikbaar = "beschikbaar"
    niet_beschikbaar = "niet_beschikbaar"
    discontinued = "discontinued"


@dataclass(slots=True, eq=False)
class Table:
    id: str
    fields: tuple[Field, ...] = ()
    scopes: frozenset[Scope] = frozenset()
    dataset: Dataset = dataclass_field(kw_only=True, repr=False)
    related_datasets: tuple[Dataset, ...] = dataclass_field(default=(), kw_only=True, repr=False)
    parent_table: Table | None = dataclass_field(default=None, kw_only=True, repr=False)
    parent_table_field: Field | None = dataclass_field(default=None, kw_only=True, repr=False)
    version: SemVer = dataclass_field(default_factory=lambda: SemVer("1"), kw_only=True)
    title: str | None = dataclass_field(default=None, kw_only=True)
    description: str | None = dataclass_field(default=None, kw_only=True)
    shortname_override: str | None = dataclass_field(default=None, kw_only=True)
    crs_override: str | None = dataclass_field(default=None, kw_only=True)
    status: TableStatus = dataclass_field(default=TableStatus.stable, kw_only=True)
    derived_from: tuple[str, ...] = dataclass_field(default=(), kw_only=True)
    identifier_fields: tuple[Field, ...] = dataclass_field(default=(), kw_only=True)
    display_field: Field | None = dataclass_field(default=None, kw_only=True)
    temporal: Temporal | None = dataclass_field(default=None, kw_only=True)
    main_geometry_field: Field | None = dataclass_field(default=None, kw_only=True)
    related_main_geometry_field: Field | None = dataclass_field(
        default=None, kw_only=True, repr=False
    )
    main_geometry_explicit: bool = dataclass_field(default=False, kw_only=True)
    kind: TableKind = dataclass_field(default=TableKind.REGULAR, kw_only=True)
    through_fields: tuple[Field, Field] | None = dataclass_field(default=None, kw_only=True)
    subresources: tuple[Subresource, ...] = dataclass_field(default=(), kw_only=True)
    additional_relations: tuple[AdditionalRelation, ...] = dataclass_field(
        default=(), kw_only=True
    )
    rla: RowLevelAuthorisation | None = dataclass_field(default=None, kw_only=True)
    max_zoom: int = dataclass_field(default=30, kw_only=True)
    min_zoom: int = dataclass_field(default=15, kw_only=True)
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)
    schema_metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)

    def __post_init__(self) -> None:
        if not isinstance(self.kind, TableKind):
            raise TypeError(f"kind must be a TableKind, got {self.kind!r}.")

    @property
    def shortname(self) -> str:
        return self.shortname_override if self.shortname_override is not None else self.id

    @property
    def crs(self) -> str | None:
        return self.crs_override or self.dataset.crs

    @property
    def identifier(self) -> list[str]:
        return [field.id for field in self.identifier_fields]

    @property
    def has_composite_key(self) -> bool:
        return len(self.identifier_fields) > 1

    @property
    def is_temporal(self) -> bool:
        return self.temporal is not None

    @property
    def has_main_geometry(self) -> bool:
        return self.main_geometry_explicit

    @property
    def has_relation_as_main_geometry(self) -> bool:
        return self.has_main_geometry and self.related_main_geometry_field is not None

    @property
    def has_geometry_fields(self) -> bool:
        return any(field.is_geo for field in self.fields) or self.has_relation_as_main_geometry

    @property
    def related_dataset_ids(self) -> set[str]:
        return {dataset.id for dataset in self.related_datasets}

    def get_field_by_id(self, field_id: str) -> Field:
        for field in self.fields:
            if field.id == field_id:
                return field
        raise DatasetFieldNotFound(f"Field {field_id!r} does not exist in table {self.id!r}.")

    def get_additional_relation_by_id(self, relation_id: str) -> AdditionalRelation:
        for relation in self.additional_relations:
            if relation.id == relation_id:
                return relation
        raise DatasetFieldNotFound(f"Relation {relation_id!r} does not exist in {self.id!r}.")

    def get_reverse_relation(self, field: Field) -> AdditionalRelation | None:
        if not field.is_relation:
            raise ValueError("Field is not a relation")
        return next(
            (rel for rel in self.additional_relations if rel.is_reverse_relation(field)), None
        )


class VersionStatus(Enum):
    stable = "stable"
    under_development = "under_development"
    superseded = "superseded"
    deprecated = "deprecated"
    discontinued = "discontinued"


@dataclass(slots=True, eq=False)
class DatasetVersion:
    id: str
    tables: tuple[Table, ...] = ()
    Status: ClassVar[type[VersionStatus]] = VersionStatus
    dataset: Dataset = dataclass_field(kw_only=True, repr=False)
    status: VersionStatus = dataclass_field(default=VersionStatus.stable, kw_only=True)
    is_default: bool = dataclass_field(default=False, kw_only=True)
    enable_api: bool = dataclass_field(default=False, kw_only=True)
    enable_geosearch: bool = dataclass_field(default=False, kw_only=True)
    end_support_date: date | None = dataclass_field(default=None, kw_only=True)
    release_date: date | None = dataclass_field(default=None, kw_only=True)
    exports: tuple[Export, ...] = dataclass_field(default=(), kw_only=True)
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)

    @property
    def version(self) -> str:
        return self.id

    @property
    def nested_tables(self) -> list[Table]:
        return [
            field.nested_table
            for table in self.tables
            for field in table.fields
            if field.nested_table is not None
        ]

    @property
    def through_tables(self) -> list[Table]:
        return [
            field.through_table
            for table in self.tables
            for field in table.fields
            if field.through_table is not None
        ]

    def get_tables(
        self, include_nested: bool = False, include_through: bool = False
    ) -> list[Table]:
        return [
            *self.tables,
            *(self.nested_tables if include_nested else ()),
            *(self.through_tables if include_through else ()),
        ]

    def get_table_by_id(
        self,
        table_id: str,
        include_nested: bool = True,
        include_through: bool = True,
    ) -> Table:
        for table in self.get_tables(include_nested, include_through):
            if table.id == table_id:
                return table
        raise DatasetTableNotFound(f"Table {table_id!r} does not exist in version {self.id!r}.")


@dataclass(slots=True, eq=False)
class Dataset:
    id: str
    versions: tuple[DatasetVersion, ...] = ()
    default_version_id: str | None = None
    scopes: frozenset[Scope] = frozenset()
    publisher: Publisher | None = None
    title: str | None = dataclass_field(default=None, kw_only=True)
    description: str | None = dataclass_field(default=None, kw_only=True)
    license: str | None = dataclass_field(default=None, kw_only=True)
    crs: str | None = dataclass_field(default=None, kw_only=True)
    view_sql: str | None = dataclass_field(default=None, kw_only=True)
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)

    @property
    def default_version(self) -> str:
        return self.default_version_id if self.default_version_id is not None else "v1"

    def get_version(self, version: str) -> DatasetVersion:
        for item in self.versions:
            if item.id == version:
                return item
        raise DatasetVersionNotFound(f"Version {version!r} does not exist in dataset {self.id!r}.")

    @property
    def tables(self) -> list[Table]:
        return self.get_tables()

    @property
    def table_ids(self) -> list[str]:
        return list(
            dict.fromkeys(table.id for version in self.versions for table in version.tables)
        )

    def get_tables(
        self,
        version: str | None = None,
        include_nested: bool = False,
        include_through: bool = False,
    ) -> list[Table]:
        return self.get_version(version or self.default_version).get_tables(
            include_nested, include_through
        )

    def get_all_tables(
        self, include_nested: bool = False, include_through: bool = False
    ) -> list[Table]:
        """Return distinct table nodes in version order, not physical SQL tables."""
        return list(
            dict.fromkeys(
                table
                for version in self.versions
                for table in version.get_tables(include_nested, include_through)
            )
        )

    def get_table_by_id(
        self,
        table_id: str,
        version: str | None = None,
        include_nested: bool = True,
        include_through: bool = True,
    ) -> Table:
        return self.get_version(version or self.default_version).get_table_by_id(
            table_id,
            include_nested,
            include_through,
        )

    @property
    def nested_tables(self) -> list[Table]:
        return self.get_version(self.default_version).nested_tables

    @property
    def through_tables(self) -> list[Table]:
        return self.get_version(self.default_version).through_tables

    @property
    def related_dataset_schema_ids(self) -> set[str]:
        return {id_ for table in self.tables for id_ in table.related_dataset_ids}


@dataclass(frozen=True, slots=True)
class Publisher:
    id: str
    name: str = ""
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)


@dataclass(frozen=True, slots=True, eq=False)
class Scope:
    id: str
    name: str = ""
    owner: Publisher | None = None
    production_package: str = ""
    non_production_package: str = ""
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)

    def __str__(self) -> str:
        return self.id

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Scope) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass(slots=True, eq=False)
class Profile:
    id: str
    name: str | None = None
    scopes: frozenset[Scope] = frozenset()
    datasets: Mapping[str, ProfileDataset] = dataclass_field(default_factory=dict, kw_only=True)
    metadata: Mapping[str, Any] = dataclass_field(default_factory=dict, kw_only=True)


class RelationKind(Enum):
    """Cardinality/type using the schema's relation terminology."""

    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"
    MANY_TO_MANY = "many-to-many"


@dataclass(frozen=True, slots=True)
class TableRelation:
    """A resolved edge owned by its declaring field and targeting a table."""

    target: Table
    kind: RelationKind
    through_table: Table | None = None
    target_fields: tuple[Field, ...] | None = None
    reverse: AdditionalRelation | None = None
    loose: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.kind, RelationKind):
            raise TypeError(f"kind must be a RelationKind, got {self.kind!r}.")


class TemporalDimensionFields(NamedTuple):
    start: Field
    end: Field


@dataclass(slots=True)
class Temporal:
    identifier_field: Field
    dimensions: Mapping[str, TemporalDimensionFields] = dataclass_field(default_factory=dict)

    @property
    def identifier(self) -> str:
        return self.identifier_field.id

    @property
    def temporal_fields(self) -> list[Field]:
        return [field for fields in self.dimensions.values() for field in fields]


class Subresource(NamedTuple):
    table: Table
    field: Field


@dataclass(slots=True, eq=False)
class AdditionalRelation:
    id: str
    parent_table: Table
    related_table: Table
    related_field: Field
    format: Literal["summary", "embedded"] = "summary"
    related_dataset: Dataset = dataclass_field(kw_only=True, repr=False)

    def is_reverse_relation(self, field: Field) -> bool:
        return self.related_field is field


@dataclass(slots=True)
class RowLevelAuthorisation:
    targets: tuple[Field, ...]
    source: Field
    auth_map: Mapping[bool, frozenset[Scope]]


@dataclass(slots=True)
class ProfileDataset:
    target: Dataset
    permissions: Permission = dataclass_field(
        default_factory=lambda: Permission(PermissionLevel.NONE)
    )
    tables: Mapping[str, ProfileTable] = dataclass_field(default_factory=dict)
    profile: Profile = dataclass_field(kw_only=True, repr=False)

    @property
    def id(self) -> str:
        return self.target.id


@dataclass(slots=True)
class ProfileTable:
    target: Table
    permissions: Permission
    field_permissions: Mapping[Field, Permission] = dataclass_field(default_factory=dict)
    mandatory_filtersets: tuple[tuple[str, ...], ...] = ()
    dataset: ProfileDataset = dataclass_field(kw_only=True, repr=False)

    @property
    def id(self) -> str:
        return self.target.id


ExportFileType = Literal["csv", "jsonl", "gpkg", "geojson"]


@dataclass(slots=True)
class Export:
    name: str
    tables: tuple[Table, ...]
    scopes: frozenset[Scope]
    filetype: ExportFileType
    version: DatasetVersion
    dataset: Dataset = dataclass_field(kw_only=True, repr=False)

    @classmethod
    def from_table(
        cls,
        table: Table,
        filetype: ExportFileType,
        scopes: Iterable[Scope],
        *,
        dataset: Dataset,
        version: DatasetVersion,
    ) -> Export:
        if not any(table is item for item in version.tables):
            raise DatasetTableNotFound(f"Table {table.id!r} is not in version {version.id!r}.")
        return cls(table.id, (table,), frozenset(scopes), filetype, version, dataset=dataset)

    def __str__(self) -> str:
        scopes = ", ".join(sorted(scope.name for scope in self.scopes))
        return (
            f"<Export: {self.name} ({self.filetype}) for {self.dataset.id} "
            f"{self.version.id} with scopes {scopes}>"
        )

    @property
    def table_ids(self) -> list[str]:
        return [table.id for table in self.tables]

    @property
    def is_public(self) -> bool:
        return len(self.scopes) == 1 and next(iter(self.scopes)).id == "OPENBAAR"


class _Identified(Protocol):
    @property
    def id(self) -> str: ...


IdentifiedT = TypeVar("IdentifiedT", bound=_Identified)


def _index_by_id(objects: Iterable[IdentifiedT], kind: str) -> Mapping[str, IdentifiedT]:
    indexed = {}
    for item in objects:
        if item.id in indexed:
            raise ValueError(f"Duplicate {kind} id {item.id!r} in dataset graph.")
        indexed[item.id] = item
    return MappingProxyType(indexed)


class DatasetGraph:
    """An I/O-free container for materialized nodes and resolved edges.

    Validation is a separate workflow over authored definitions. Construction
    indexes the supplied nodes without inspecting or resolving their references.
    Iteration yields datasets in insertion order.
    """

    def __init__(
        self,
        datasets: Iterable[Dataset],
        *,
        publishers: Iterable[Publisher] = (),
        scopes: Iterable[Scope] = (),
        profiles: Iterable[Profile] = (),
    ) -> None:
        self.datasets = _index_by_id(datasets, "dataset")
        self.publishers = _index_by_id(publishers, "publisher")
        self.scopes = _index_by_id(scopes, "scope")
        self.profiles = _index_by_id(profiles, "profile")

    def __iter__(self) -> Iterator[Dataset]:
        return iter(self.datasets.values())

    def get_dataset(self, dataset_id: str) -> Dataset:
        try:
            return self.datasets[dataset_id]
        except KeyError:
            raise DatasetNotFound(f"Dataset {dataset_id!r} is not in this graph.") from None
