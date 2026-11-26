from datetime import date

import pytest

from schematools.domain.graph.objects import (
    AdditionalRelation,
    Dataset,
    DatasetGraph,
    DatasetVersion,
    Export,
    Field,
    Permission,
    PermissionLevel,
    Profile,
    ProfileDataset,
    ProfileTable,
    Publisher,
    RelationKind,
    RowLevelAuthorisation,
    Scope,
    SemVer,
    Subresource,
    Table,
    TableKind,
    TableRelation,
    Temporal,
    TemporalDimensionFields,
)
from schematools.exceptions import (
    DatasetFieldNotFound,
    DatasetNotFound,
    DatasetTableNotFound,
    DatasetVersionNotFound,
)
from schematools.types import DatasetSchema


def materialized_graph(datasets, **kwargs):
    """Act as a test adapter: finish wiring nodes before publishing a graph."""

    def wire_fields(table, fields, parent=None):
        for field in fields:
            field.parent_field = parent
            wire_fields(table, field.subfields, field)

    for dataset in datasets:
        for version in dataset.versions:
            version.is_default = version.id == dataset.default_version
            for table in version.tables:
                for field in table.fields:
                    for projected in (field.nested_table, field.through_table):
                        if projected is not None:
                            projected.parent_table = table
                            projected.parent_table_field = field
            for table in version.get_tables(True, True):
                wire_fields(table, table.fields)
    return DatasetGraph(datasets, **kwargs)


def test_dataset_graph_follows_materialized_edges():
    publisher = Publisher("agency", "Agency")
    scope = Scope("public", "Public", owner=publisher)
    scopes = frozenset({scope})
    root = Dataset("roads", default_version_id="v1", scopes=scopes, publisher=publisher)
    related = Dataset("areas", default_version_id="v1")
    related_table = Table("segments", dataset=related)
    root_table = Table("routes", dataset=root, scopes=scopes)
    related_field = Field(
        "segment",
        table=root_table,
        relation=TableRelation(
            target=related_table,
            kind=RelationKind.ONE_TO_MANY,
        ),
        scopes=scopes,
    )
    root_table.fields = (related_field,)
    root.versions = (DatasetVersion("v1", (root_table,), dataset=root),)
    related.versions = (DatasetVersion("v1", (related_table,), dataset=related),)
    profile = Profile("reader", "Reader", scopes)
    graph = materialized_graph([root, related], publishers=[publisher], scopes=[scope])

    assert graph.get_dataset("roads") is root
    assert related.get_table_by_id("segments") is related_table
    assert related_field.relation is not None
    assert related_field.relation.target is related_table
    assert graph.publishers["agency"] is publisher
    assert graph.scopes["public"] is scope
    assert next(iter(related_field.scopes)) is scope
    assert next(iter(root_table.scopes)) is scope
    assert next(iter(root.scopes)) is scope
    assert next(iter(profile.scopes)) is scope
    assert root.publisher is publisher
    assert scope.owner is publisher


def test_dataset_local_table_lookup_uses_selected_version():
    dataset = Dataset("roads")
    dataset.versions = (
        DatasetVersion("v1", (Table("routes", dataset=dataset),), dataset=dataset),
        DatasetVersion("v2", (Table("routes", dataset=dataset),), dataset=dataset),
    )

    assert dataset.get_table_by_id("routes") is dataset.versions[0].tables[0]
    assert dataset.get_table_by_id("routes", version="v2") is dataset.versions[1].tables[0]
    with pytest.raises(DatasetVersionNotFound):
        dataset.get_version("v3")
    with pytest.raises(DatasetTableNotFound):
        dataset.get_table_by_id("missing")


def test_all_tables_uses_node_identity_instead_of_database_names():
    dataset = Dataset("roads")
    first = Table("routes", dataset=dataset)
    second = Table("routes", dataset=dataset)
    shared = Table("shared", dataset=dataset)
    dataset.versions = (
        DatasetVersion("v1", (first, shared), dataset=dataset),
        DatasetVersion("v2", (second, shared), dataset=dataset),
    )

    assert dataset.get_all_tables() == [first, shared, second]


def test_table_lookup_requires_canonical_identifiers():
    dataset = Dataset("cityAreas")
    table = Table("areaParts", dataset=dataset)
    dataset.versions = (DatasetVersion("v1", (table,), dataset=dataset),)

    assert dataset.get_table_by_id("areaParts") is table
    with pytest.raises(DatasetTableNotFound):
        dataset.get_table_by_id("area_parts")
    with pytest.raises(DatasetTableNotFound):
        dataset.get_table_by_id("AreaParts")


def test_identifier_membership_uses_canonical_fields_not_sql_id_conventions(domain_dataset):
    table = domain_dataset.tables[0]
    generated_id = table.get_field_by_id("id")
    identifier = table.identifier_fields[0]
    same_id = Field(identifier.id, table=table)

    assert identifier.is_identifier_part
    assert not generated_id.is_identifier_part
    assert not same_id.is_identifier_part


@pytest.mark.parametrize("dataset_ids", [(), ("roads", "areas")])
def test_dataset_graph_iteration_yields_datasets(dataset_ids):
    datasets = [Dataset(dataset_id) for dataset_id in dataset_ids]
    graph = DatasetGraph(iter(datasets))

    assert list(graph) == datasets
    assert list(graph) == datasets


def test_dataset_graph_raises_for_missing_objects():
    graph = DatasetGraph([])

    with pytest.raises(DatasetNotFound):
        graph.get_dataset("missing")


def test_dataset_graph_rejects_duplicate_dataset_identity():
    with pytest.raises(ValueError, match="Duplicate dataset id"):
        DatasetGraph([Dataset("roads"), Dataset("roads")])


def test_dataset_graph_construction_does_not_validate_or_wire_references():
    root = Dataset("roads")
    root_table = Table("routes", dataset=root)
    target = Table("segments", dataset=Dataset("areas"))
    source = Field(
        "segment",
        table=root_table,
        relation=TableRelation(
            target=target,
            kind=RelationKind.ONE_TO_MANY,
        ),
    )
    root_table.fields = (source,)
    version = DatasetVersion("v1", (root_table,), dataset=root)
    root.versions = (version,)

    graph = DatasetGraph([root])

    assert graph.get_dataset("roads") is root
    assert source.relation is not None
    assert source.relation.target.id == "segments"
    assert source.table is root_table
    assert version.dataset is root
    assert not version.is_default


def test_dataset_graph_materializes_many_to_many_through_table():
    dataset = Dataset("network")
    through_table = Table("roads_areas", dataset=dataset)
    root_table = Table("roads", dataset=dataset)
    relation = TableRelation(
        target=Table("areas", dataset=dataset),
        kind=RelationKind.MANY_TO_MANY,
        through_table=through_table,
    )
    source = Field(
        "related_areas",
        relation=relation,
        table=root_table,
    )
    root_table.fields = (source,)
    target_table = relation.target
    dataset.versions = (
        DatasetVersion("v1", (root_table, target_table, through_table), dataset=dataset),
    )

    graph = materialized_graph([dataset])

    assert graph.get_dataset("network") is dataset
    assert source.relation is relation
    assert source.relation.target is target_table
    assert relation.kind is RelationKind.MANY_TO_MANY
    assert relation.through_table is through_table


def test_field_owns_a_self_relation():
    dataset = Dataset("nodes")
    table = Table("nodes", dataset=dataset)
    field = Field("parent", table=table)
    table.fields = (field,)
    relation = TableRelation(target=table, kind=RelationKind.ONE_TO_ONE)
    field.relation = relation
    dataset.versions = (DatasetVersion("v1", (table,), dataset=dataset),)
    graph = materialized_graph([dataset])

    assert graph.get_dataset("nodes").versions[0].tables[0] is table
    assert field.relation is relation
    assert field.relation.target is table
    assert relation.kind is RelationKind.ONE_TO_ONE


def test_dataset_graph_indexes_shared_nodes_without_validating_references():
    publisher = Publisher("agency")
    scope = Scope("restricted", owner=publisher)
    dataset = Dataset("catalog", scopes=frozenset({scope}), publisher=publisher)
    indexed_publisher = Publisher("agency")
    indexed_scope = Scope("restricted")
    profile = Profile("reader")
    profile_dataset = ProfileDataset(dataset, profile=profile)
    profile_table = ProfileTable(
        Table("items", dataset=dataset),
        Permission(PermissionLevel.NONE),
        dataset=profile_dataset,
    )
    profile_dataset.tables = {"items": profile_table}
    profile.datasets = {"catalog": profile_dataset}

    graph = DatasetGraph(
        [dataset],
        publishers=[indexed_publisher],
        scopes=[indexed_scope],
        profiles=[profile],
    )

    assert graph.publishers["agency"] is indexed_publisher
    assert graph.scopes["restricted"] is indexed_scope
    assert graph.profiles["reader"] is profile
    assert dataset.publisher is publisher
    assert next(iter(dataset.scopes)) is scope
    assert scope.owner is publisher
    assert profile_dataset.profile is profile
    assert profile_table.dataset is profile_dataset


@pytest.fixture
def domain_dataset():
    public = Scope("OPENBAAR", "OPENBAAR")
    scopes = frozenset({public})
    dataset = Dataset(
        "cityAreas",
        default_version_id="v2",
        scopes=scopes,
        title="City areas",
        description="Areas in the city",
        license="public",
        crs="EPSG:28992",
    )
    table = Table(
        "areaParts",
        dataset=dataset,
        scopes=scopes,
        version=SemVer("2.3.4"),
        main_geometry_explicit=True,
        title="Areas",
        description="Area parts",
        shortname_override="parts",
    )
    generated_id = Field("id", table=table, type="string", required=True, scopes=scopes)
    identifier = Field("identificatie", table=table, type="string", required=True, scopes=scopes)
    sequence = Field("volgnummer", table=table, type="integer", required=True, scopes=scopes)
    start = Field("beginGeldigheid", table=table, type="string", format="date-time", scopes=scopes)
    end = Field("eindGeldigheid", table=table, type="string", format="date-time", scopes=scopes)
    geometry = Field(
        "geometrie",
        table=table,
        type="https://geojson.org/schema/Point.json",
        scopes=scopes,
    )
    address = Field("address", table=table, type="object", scopes=scopes)
    street = Field("streetName", table=table, type="string", scopes=scopes, parent_field=address)
    address.subfields = (street,)
    table.fields = (generated_id, identifier, sequence, start, end, geometry, address)
    table.identifier_fields = (identifier, sequence)
    table.temporal = Temporal(sequence, {"valid": TemporalDimensionFields(start, end)})
    table.main_geometry_field = geometry
    version = DatasetVersion(
        "v2",
        (table,),
        dataset=dataset,
        enable_api=True,
        enable_geosearch=True,
        is_default=True,
        release_date=date(2026, 11, 18),
    )
    dataset.versions = (version,)
    return dataset


@pytest.fixture
def legacy_dataset():
    return DatasetSchema(
        {
            "id": "cityAreas",
            "type": "dataset",
            "title": "City areas",
            "description": "Areas in the city",
            "license": "public",
            "crs": "EPSG:28992",
            "auth": ["OPENBAAR"],
            "defaultVersion": "v2",
            "versions": {
                "v2": {
                    "status": "stable",
                    "enableAPI": True,
                    "enableGeosearch": True,
                    "releaseDate": "2026-11-18",
                    "tables": [
                        {
                            "id": "areaParts",
                            "type": "table",
                            "version": "2.3.4",
                            "title": "Areas",
                            "description": "Area parts",
                            "shortname": "parts",
                            "temporal": {
                                "identifier": "volgnummer",
                                "dimensions": {
                                    "valid": ["beginGeldigheid", "eindGeldigheid"],
                                },
                            },
                            "schema": {
                                "$schema": "http://json-schema.org/draft-07/schema#",
                                "type": "object",
                                "identifier": ["identificatie", "volgnummer"],
                                "required": ["id", "identificatie", "volgnummer"],
                                "mainGeometry": "geometrie",
                                "properties": {
                                    "id": {"type": "string"},
                                    "identificatie": {"type": "string"},
                                    "volgnummer": {"type": "integer"},
                                    "beginGeldigheid": {
                                        "type": "string",
                                        "format": "date-time",
                                    },
                                    "eindGeldigheid": {
                                        "type": "string",
                                        "format": "date-time",
                                    },
                                    "geometrie": {
                                        "$ref": "https://geojson.org/schema/Point.json",
                                    },
                                    "address": {
                                        "type": "object",
                                        "properties": {"streetName": {"type": "string"}},
                                    },
                                },
                            },
                        },
                    ],
                },
            },
        }
    )


@pytest.mark.parametrize(
    "attribute",
    [
        "title",
        "description",
        "license",
        "crs",
        "default_version",
    ],
)
def test_dataset_metadata_matches_legacy(domain_dataset, legacy_dataset, attribute):
    assert getattr(domain_dataset, attribute) == (
        legacy_dataset.get("crs") if attribute == "crs" else getattr(legacy_dataset, attribute)
    )


@pytest.mark.parametrize(
    "attribute",
    [
        "shortname",
        "title",
        "description",
        "crs",
        "is_temporal",
        "identifier",
        "has_composite_key",
        "has_main_geometry",
        "has_relation_as_main_geometry",
        "has_geometry_fields",
        "min_zoom",
        "max_zoom",
    ],
)
def test_table_behavior_matches_legacy(domain_dataset, legacy_dataset, attribute):
    assert getattr(domain_dataset.tables[0], attribute) == getattr(
        legacy_dataset.tables[0], attribute
    )


@pytest.mark.parametrize(
    "field_id",
    ["id", "identificatie", "volgnummer", "beginGeldigheid", "geometrie", "address"],
)
def test_field_behavior_matches_legacy(domain_dataset, legacy_dataset, field_id):
    field = domain_dataset.tables[0].get_field_by_id(field_id)
    old_field = legacy_dataset.tables[0].get_field_by_id(field_id)
    for attribute in (
        "type",
        "required",
        "is_object",
        "is_scalar",
        "is_array",
        "is_subfield",
        "is_nested_object",
        "is_array_of_objects",
        "is_array_of_scalars",
        "is_geo",
        "crs",
        "srid",
        "format",
    ):
        assert getattr(field, attribute) == getattr(old_field, attribute), attribute


def test_local_navigation_temporal_geometry_and_subfields(domain_dataset):
    table = domain_dataset.tables[0]
    address = table.get_field_by_id("address")
    street = address.get_field_by_id("streetName")
    assert table.dataset is domain_dataset
    assert street.table is table
    assert street.parent_field is address
    assert table.temporal is not None
    assert table.temporal.identifier_field is table.identifier_fields[1]
    assert table.temporal.temporal_fields == [
        table.get_field_by_id("beginGeldigheid"),
        table.get_field_by_id("eindGeldigheid"),
    ]
    assert table.main_geometry_field is table.get_field_by_id("geometrie")
    assert table.main_geometry_field.srid == 28992
    assert table.related_main_geometry_field is None
    assert not table.has_relation_as_main_geometry
    assert address in table.fields
    assert address.subfields == (street,)
    with pytest.raises(DatasetFieldNotFound):
        address.get_field_by_id("missing")
    with pytest.raises(DatasetFieldNotFound):
        table.get_field_by_id("missing")


def test_related_main_geometry_uses_its_materialized_reference(domain_dataset, monkeypatch):
    target = domain_dataset.tables[0]
    geometry = target.main_geometry_field
    source = Table(
        "geometryLinks",
        dataset=domain_dataset,
        main_geometry_explicit=True,
        related_main_geometry_field=geometry,
    )
    link = Field(
        "geometry",
        table=source,
        relation=TableRelation(target, RelationKind.ONE_TO_MANY),
    )
    source.main_geometry_field = link
    source.fields = (link,)
    domain_dataset.versions[0].tables += (source,)
    graph = DatasetGraph([domain_dataset], scopes=source.scopes)

    def unexpected_traversal(field):
        pytest.fail("Geometry access must use the materialized reference.")

    monkeypatch.setattr(Field, "related_table", property(unexpected_traversal))

    materialized = graph.get_dataset("cityAreas").get_table_by_id("geometryLinks")
    assert geometry is not None
    assert materialized.related_main_geometry_field is geometry
    assert materialized.has_relation_as_main_geometry
    assert materialized.has_geometry_fields


def test_resolved_temporal_relation_and_reverse_navigation(domain_dataset):
    target = domain_dataset.tables[0]
    source = Table("roads", dataset=domain_dataset, related_datasets=(domain_dataset,))
    field = Field("area", table=source, type="object")
    key = Field("identificatie", table=source, parent_field=field, type="string")
    sequence = Field("volgnummer", table=source, parent_field=field, type="integer")
    date_field = Field(
        "beginGeldigheid", table=source, parent_field=field, type="string", format="date-time"
    )
    field.subfields = (key, sequence, date_field)
    source.fields = (field,)
    reverse = AdditionalRelation("roads", target, source, field, related_dataset=domain_dataset)
    target.additional_relations = (reverse,)
    field.relation = TableRelation(
        target,
        RelationKind.ONE_TO_MANY,
        target_fields=target.identifier_fields,
        reverse=reverse,
    )
    assert field.related_table is target
    assert field.related_fields is target.identifier_fields
    assert field.related_field_ids == ["identificatie", "volgnummer"]
    assert field.reverse_relation is reverse
    assert target.get_reverse_relation(field) is reverse
    assert reverse.related_field is field
    assert field.is_composite_key
    assert not field.is_loose_relation
    assert date_field.is_temporal_range
    assert field.subfields == (key, sequence, date_field)
    assert field.relation_attributes == {"beginGeldigheid"}
    assert source.related_dataset_ids == {"cityAreas"}


def test_materialized_nested_and_through_tables_are_reused(domain_dataset):
    table = domain_dataset.tables[0]
    nested = Table("areaParts_items", kind=TableKind.NESTED, dataset=domain_dataset)
    through = Table("areaParts_links", kind=TableKind.THROUGH, dataset=domain_dataset)
    nested_field = Field(
        "items", table=table, type="array", item_type="object", nested_table=nested
    )
    m2m = Field(
        "links",
        table=table,
        type="array",
        relation=TableRelation(table, RelationKind.MANY_TO_MANY, through_table=through),
    )
    table.fields += (nested_field, m2m)
    version = domain_dataset.versions[0]
    assert version.nested_tables == [nested]
    assert version.through_tables == [through]
    assert table.kind is TableKind.REGULAR
    assert nested.kind is TableKind.NESTED
    assert through.kind is TableKind.THROUGH
    assert version.get_table_by_id("areaParts_items") is nested
    assert version.get_table_by_id("areaParts_links") is through
    assert nested_field.nested_table is nested
    assert m2m.through_table is through
    graph = materialized_graph([domain_dataset], scopes=table.scopes)
    assert graph.get_dataset(domain_dataset.id).nested_tables == [nested]


def test_profiles_permissions_exports_and_row_authorisation(domain_dataset):
    table = domain_dataset.tables[0]
    version = domain_dataset.versions[0]
    field = table.fields[0]
    permission = Permission.from_string("letters:3", source="profile")
    profile = Profile("reader", "Reader", table.scopes)
    profile_dataset = ProfileDataset(domain_dataset, profile=profile)
    profile.datasets = {domain_dataset.id: profile_dataset}
    effective_permission = Permission(
        PermissionLevel.SUBOBJECTS_ONLY,
        source="profiles[Reader].datasets.cityAreas.tables.areaParts.fields.*",
    )
    profile_table = ProfileTable(
        table,
        effective_permission,
        dataset=profile_dataset,
        field_permissions={field: permission},
        mandatory_filtersets=(("name",),),
    )
    profile_dataset.tables = {table.id: profile_table}
    export = Export("areas", (table,), table.scopes, "csv", version, dataset=domain_dataset)
    version.exports = (export,)
    table.rla = RowLevelAuthorisation((field,), field, {True: table.scopes})
    graph = DatasetGraph([domain_dataset], scopes=table.scopes, profiles=[profile])
    assert graph.profiles["reader"] is profile
    assert profile.datasets[domain_dataset.id].target is domain_dataset
    assert profile_table.permissions is effective_permission
    assert profile_table.field_permissions[field] is permission
    transform = permission.transform_function()
    assert transform is not None
    assert transform("abcdef") == "abc"
    assert export.is_public
    assert export.tables == (table,)
    assert table.rla.source is field
    assert table.rla.auth_map[True] is table.scopes


@pytest.mark.parametrize("cls", [Dataset, Table, Field, Profile])
def test_domain_nodes_do_not_expose_auth(cls):
    assert not hasattr(cls, "auth")


@pytest.mark.parametrize("cls", [Field, Table, DatasetVersion])
def test_domain_nodes_do_not_expose_schema(cls):
    assert not hasattr(cls, "schema")


def test_materialized_context_does_not_require_ancestor_navigation(domain_dataset):
    table = domain_dataset.tables[0]
    version = domain_dataset.versions[0]
    domain_dataset.versions = ()
    source = Table("roads", dataset=domain_dataset, related_datasets=(domain_dataset,))
    field = Field("area", table=source)
    reverse = AdditionalRelation("roads", table, source, field, related_dataset=domain_dataset)
    export = Export.from_table(table, "csv", table.scopes, dataset=domain_dataset, version=version)

    assert version.is_default
    assert source.related_dataset_ids == {"cityAreas"}
    assert reverse.related_dataset is domain_dataset
    assert reverse.related_table is source
    assert reverse.related_field is field
    assert export.dataset is domain_dataset
    assert export.version is version
    assert export.tables == (table,)
    assert str(export) == "<Export: areaParts (csv) for cityAreas v2 with scopes OPENBAAR>"


@pytest.mark.parametrize(
    "cls", [Dataset, DatasetVersion, Table, Field, Scope, Publisher, Profile, AdditionalRelation]
)
def test_domain_nodes_do_not_expose_projection_naming(cls):
    assert not hasattr(cls, "url_safe_id")
    assert not hasattr(cls, "db_name")
    assert not hasattr(cls, "python_name")
    assert not hasattr(cls, "db_name_variant")
    assert not hasattr(cls, "get_view_user")


@pytest.mark.parametrize(
    ("cls", "removed"),
    [
        (
            Field,
            (
                "name",
                "qualified_id",
                "description_with_unit",
                "is_primary",
                "is_internal",
                "is_autoincrement",
                "is_nested_table",
                "is_through_table",
                "is_json_object",
                "filter_auth",
            ),
        ),
        (
            Table,
            (
                "get_fields",
                "get_db_fields",
                "qualified_id",
                "is_view",
                "is_autoincrement",
                "main_geometry",
                "is_nested_table",
                "is_through_table",
                "has_parent_table",
            ),
        ),
        (
            Dataset,
            (
                "get_view_sql",
                "filter_on_tables",
                "filter_on_scopes",
                "has_an_available_version",
                "has_an_available_geosearch_version",
            ),
        ),
        (Scope, ("accessPackages", "productionPackage", "nonProductionPackage")),
        (AdditionalRelation, ("name", "relation")),
        (ProfileTable, ("fields",)),
        (
            Export,
            ("scopes_string", "filename", "filename_without_zip", "table_filename", "table_paths"),
        ),
    ],
)
def test_domain_surface_does_not_choose_db_or_api_representations(cls, removed):
    for name in removed:
        assert not hasattr(cls, name)


def test_dataset_graph_construction_does_not_traverse_nodes(domain_dataset, monkeypatch):
    def unexpected_traversal(*args, **kwargs):
        pytest.fail("Graph construction must not traverse dataset tables.")

    monkeypatch.setattr(DatasetVersion, "get_tables", unexpected_traversal)

    graph = DatasetGraph([domain_dataset])

    assert graph.get_dataset(domain_dataset.id) is domain_dataset


def test_domain_surface_does_not_expose_ambient_loading():
    for cls in (Dataset, DatasetVersion, Table, Field, Scope, Publisher, Profile):
        assert not hasattr(cls, "loader")
        assert not hasattr(cls, "from_file")
        assert not hasattr(cls, "from_dict")
    assert not hasattr(DatasetGraph, "get_related_table")


@pytest.mark.parametrize(
    "cls",
    [
        DatasetGraph,
        Dataset,
        DatasetVersion,
        Table,
        Field,
        Publisher,
        Scope,
        Profile,
        ProfileDataset,
        ProfileTable,
        TableRelation,
        AdditionalRelation,
        Temporal,
        TemporalDimensionFields,
        Subresource,
        RowLevelAuthorisation,
        Export,
    ],
)
def test_domain_surface_does_not_expose_serialization(cls):
    assert not hasattr(cls, "json")
    assert not hasattr(cls, "json_data")


@pytest.mark.parametrize(
    ("field_type", "item_type", "expected"),
    [
        ("string", None, (False, False, False)),
        ("object", None, (False, False, True)),
        ("array", "string", (True, False, False)),
        ("array", "object", (True, True, False)),
    ],
)
def test_field_structure_classification(domain_dataset, field_type, item_type, expected):
    table = domain_dataset.tables[0]
    field = Field("value", table=table, type=field_type, item_type=item_type)
    child = Field("label", table=table, parent_field=field, type="string", required=True)
    field.subfields = (child,)
    assert (field.is_array, field.is_array_of_objects, field.is_nested_object) == expected


def test_loose_temporal_relation_uses_direct_identifier_references(domain_dataset):
    target = domain_dataset.tables[0]
    source = Table("roads", dataset=domain_dataset)
    through = Table("roadAreas", dataset=domain_dataset, kind=TableKind.THROUGH)
    field = Field(
        "area",
        type="string",
        table=source,
        relation=TableRelation(
            target, RelationKind.ONE_TO_MANY, through_table=through, loose=True
        ),
    )
    assert field.is_loose_relation
    assert field.related_fields == target.identifier_fields[:1]
    assert field.related_field_ids == ["identificatie"]
    source.fields = (field,)
    version = domain_dataset.versions[0]
    version.tables += (source,)
    assert version.through_tables == [through]
    DatasetGraph([domain_dataset], scopes=target.scopes)


def test_relation_does_not_infer_looseness_from_storage_shape(domain_dataset):
    table = domain_dataset.tables[0]
    field = Field(
        "area",
        table=table,
        type="string",
        relation=TableRelation(table, RelationKind.ONE_TO_MANY),
    )
    assert not field.is_loose_relation
    assert field.related_fields is table.identifier_fields


@pytest.mark.parametrize(
    "level", [PermissionLevel.NONE, PermissionLevel.READ, PermissionLevel.SUBOBJECTS_ONLY]
)
def test_effective_profile_permissions_do_not_depend_on_parent_state(domain_dataset, level):
    profile = Profile("reader", "Reader")
    dataset = ProfileDataset(domain_dataset, profile=profile)
    permission = Permission(level, source="profiles[Reader].datasets.cityAreas.tables.areaParts")
    materialized_source = permission.source
    table = ProfileTable(domain_dataset.tables[0], permission, dataset=dataset)

    profile.name = "Renamed"
    dataset.permissions = Permission(PermissionLevel.READ)

    assert table.permissions is permission
    assert table.permissions.source == materialized_source
