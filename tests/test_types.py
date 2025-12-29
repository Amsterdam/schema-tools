from __future__ import annotations

import operator

import pytest

from schematools.exceptions import IncompatibleDataset, SchemaObjectNotFound, ScopeNotFound
from schematools.types import (
    DatasetSchema,
    DatasetTableSchema,
    Permission,
    PermissionLevel,
    ProfileSchema,
    Scope,
    SemVer,
)

from .test_loaders import HARRY_ONE_SCOPE, HARRY_THREE_SCOPE, HARRY_TWO_SCOPE


def test_permission_level_ordering() -> None:
    """Test whether enum ordering works based on the int values."""
    assert sorted(PermissionLevel) == [
        PermissionLevel.NONE,
        PermissionLevel.SUBOBJECTS_ONLY,
        PermissionLevel.LETTERS,
        PermissionLevel.RANDOM,
        PermissionLevel.ENCODED,
        PermissionLevel.READ,
    ]
    assert PermissionLevel.highest is PermissionLevel.READ
    assert PermissionLevel.highest is max(PermissionLevel)


def test_geo_and_id_when_configured(schema_loader, meetbouten_schema) -> None:
    table = meetbouten_schema.get_table_by_id("meetbouten")
    assert table.identifier == ["identificatie"]
    assert table.main_geometry == "geometrie"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_geo_and_id_when_not_configured(schema_loader, afvalwegingen_schema) -> None:
    table = afvalwegingen_schema.get_table_by_id("containers")
    assert table.identifier == ["id"]
    assert table.main_geometry == "geometry"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_import_dataset_separate_table_files(schema_loader) -> None:
    """Prove that datasets with tables in separate files are created correctly."""
    schema = schema_loader.get_dataset("gebieden_sep_tables")
    assert len(schema.tables) == 2
    table = schema.get_table_by_id("buurten")
    assert table.main_geometry == "primaireGeometrie"


def test_datasetschema_from_file_not_a_dataset(schema_loader) -> None:
    """Ensure a proper exception is raised when loading a file that's not a DatasetSchema."""

    error_msg = "Invalid Amsterdam Dataset schema file"
    with pytest.raises(ValueError, match=error_msg):
        # v1.0.0.json is a DatasetRow, not a DatasetSchema.
        schema_loader.get_dataset_from_file("gebieden_sep_tables/bouwblokken/v1.0.0.json")

    error_msg = "Invalid JSON file"
    with pytest.raises(ValueError, match=error_msg):
        # not_a_json_file.txt is not a JSON file. We should still get our ValueError.
        schema_loader.get_dataset_from_file("not_a_json_file.txt")


def test_profile_schema(brp_rname_profile_schema: ProfileSchema) -> None:
    """Prove that the profile files are properly read,
    and have their fields access the JSON data.
    """
    assert brp_rname_profile_schema.scopes == {"BRP/RNAME"}

    brp = brp_rname_profile_schema.datasets["brp"]
    table = brp.tables["ingeschrevenpersonen"]

    assert table.permissions.level is PermissionLevel.READ
    assert table.fields["bsn"] == Permission(PermissionLevel.ENCODED)
    assert table.mandatory_filtersets == [
        ["bsn", "lastname"],
        ["postcode", "lastname"],
    ]


def test_fetching_of_related_schema_ids(schema_loader) -> None:
    """Prove that ids of related dataset schemas are properly collected."""
    schema = schema_loader.get_dataset_from_file("multirelation.json")
    assert set(schema.related_dataset_schema_ids) == {"gebieden", "baseDataset"}


def test_semver_init() -> None:
    """Test SemVer initialization (including raising ValueError's)"""
    sv = SemVer("1.2.3")
    assert sv.major == 1
    assert sv.minor == 2
    assert sv.patch == 3

    sv_no_patch = SemVer("1.2")
    assert sv_no_patch.major == 1
    assert sv_no_patch.minor == 2
    assert sv_no_patch.patch == 0  # default when not explicitly specified

    sv_no_minor = SemVer("1")
    assert sv_no_minor.major == 1
    assert sv_no_minor.minor == 0  # default when not explicitly specified
    assert sv_no_minor.patch == 0  # default when not explicitly specified

    sv_with_v_prefix = SemVer("v1.2.3")
    assert sv_with_v_prefix.major == 1
    assert sv_with_v_prefix.minor == 2
    assert sv_with_v_prefix.patch == 3

    invalid_semver_values = ("1.0.0.0", "-1.0.0", "fubar")
    for isv in invalid_semver_values:
        with pytest.raises(ValueError):
            SemVer(isv)


def test_semver_str() -> None:
    """Test SemVer str representation."""
    assert str(SemVer("1.2.3")) == "1.2.3"
    assert str(SemVer("1.2")) == "1.2.0"
    assert str(SemVer("1")) == "1.0.0"

    assert str(SemVer("v1.2.3")) == "1.2.3"
    assert str(SemVer("v1.2")) == "1.2.0"
    assert str(SemVer("v1")) == "1.0.0"


def test_semver_repr() -> None:
    """Test SemVer repr representation."""
    assert repr(SemVer("1.2.3")) == 'SemVer("1.2.3")'
    assert repr(SemVer("1.2")) == 'SemVer("1.2.0")'
    assert repr(SemVer("1")) == 'SemVer("1.0.0")'

    assert repr(SemVer("v1.2.3")) == 'SemVer("1.2.3")'
    assert repr(SemVer("v1.2")) == 'SemVer("1.2.0")'
    assert repr(SemVer("v1")) == 'SemVer("1.0.0")'


def test_semver_lt() -> None:
    assert not SemVer("v1.0.0") < SemVer("1.0.0")
    assert not SemVer("1.0.0") < SemVer("v1.0.0")
    assert SemVer("v1.0.0") < SemVer("1.0.2")
    assert SemVer("v1.0.0") < SemVer("1.2.0")


def test_semver_le() -> None:
    assert SemVer("v1.0.0") <= SemVer("1.0.0")
    assert SemVer("1.0.0") <= SemVer("v1.0.0")
    assert SemVer("v1.0.0") <= SemVer("1.0.2")
    assert SemVer("v1.0.0") <= SemVer("1.2.0")


def test_semver_gt() -> None:
    assert not SemVer("v1.0.0") > SemVer("1.0.0")
    assert not SemVer("1.0.0") > SemVer("v1.0.0")
    assert SemVer("v1.0.2") > SemVer("1.0.0")
    assert SemVer("v1.2.0") > SemVer("1.0.0")


def test_semver_ge() -> None:
    assert SemVer("v1.0.0") >= SemVer("1.0.0")
    assert SemVer("1.0.0") >= SemVer("v1.0.0")
    assert SemVer("v1.0.2") >= SemVer("1.0.0")
    assert SemVer("v1.2.0") >= SemVer("1.0.0")


def test_semver_eq() -> None:
    assert SemVer("v1.0.0") == SemVer("1.0.0")
    assert SemVer("1.0.0") == SemVer("v1.0.0")
    assert SemVer("1.0.2") != SemVer("1.0.0")
    assert SemVer("1.2.0") != SemVer("1.0.0")


def test_dataset_schema_get_fields_with_surrogate_pk(
    composite_key_schema: DatasetSchema, verblijfsobjecten_schema: DatasetSchema
):
    """Prove that the surrogate 'id' key is returned once for schemas with a
    composite key, regardless of whether the surrogate key is already defined
    by the schema or generated"""

    verblijfsobjecten = verblijfsobjecten_schema.tables[0]
    composite_key_schema = composite_key_schema.tables[0]

    # this schema gets a generated 'id'
    assert sorted(x.id for x in verblijfsobjecten.get_fields()) == [
        "beginGeldigheid",
        "eindGeldigheid",
        "gebruiksdoel",
        "id",
        "identificatie",
        "ligtInBuurt",
        "schema",
        "volgnummer",
    ]

    # this schema defines an 'id'
    assert sorted(x.name for x in composite_key_schema.get_fields()) == [
        "beginGeldigheid",
        "eindGeldigheid",
        "id",
        "identificatie",
        "schema",
        "volgnummer",
    ]


def test_dataset_with_loose_1n_relations_has_no_through_tables(meldingen_schema, gebieden_schema):
    """Prove that a loose relation for a 1-N is not generating a though table.

    The `meldingen_schema` has a 1-N relation to `buurt`, however,
    that is a loose relation. Those relations should not get a through tables,
    so, the only one table that should be generated is the main meldingen table.
    """
    tables_including_through = meldingen_schema.get_tables(include_through=True)
    assert len(tables_including_through) == 1


def test_dataset_with_loose_nm_relations_has_through_tables(
    woningbouwplannen_schema, gebieden_schema
):
    """Prove that a loose relation for a NM relation is generating a though table.

    The `woningbouwplannen_schema` has two NM relations to `buurt`, however,
    those are loose relations. Those relations should get a through tables,
    because NM relations always need a through table by nature.
    So, in total, 3 tables should be created.
    """
    tables_including_through = woningbouwplannen_schema.get_tables(include_through=True)
    assert len(tables_including_through) == 3


def test_dataset_with_camel_cased_id_generates_correct_through_relations(
    schema_loader, gebieden_schema
):
    """Prove that the relation identifiers in through tables are correctly generated.

    When a through table is generated, two FK relations are inserted in this table,
    one to the source and one to the target table.
    When dataset.id or table.id in the source or target table are camelCased,
    the generated identifiers should stay intact, and should not be snakecased.
    """
    schema = schema_loader.get_dataset_from_file("multirelation.json")
    through_table = schema.through_tables[0]

    # The fields `hasrelations` and `hasNMRelation` are the FK's to source and target table.
    assert through_table.get_field_by_id("hasrelations").relation == "baseDataset:hasrelations"
    assert through_table.get_field_by_id("hasNMRelation").relation == "baseDataset:internalRelated"


def test_subfields(ggwgebieden_schema: DatasetSchema) -> None:
    field = ggwgebieden_schema.get_table_by_id("buurten").get_field_by_id("ligtInWijk")
    subfields = sorted(field.subfields, key=operator.attrgetter("id"))
    assert subfields[0].id == "identificatie"
    assert subfields[1].id == "volgnummer"

    assert subfields[0] == field.get_field_by_id("identificatie")
    assert subfields[1] == field.get_field_by_id("volgnummer")
    with pytest.raises(SchemaObjectNotFound):
        field.get_field_by_id("iDoNotExist")


def test_names_of_subobject_fields(kadastraleobjecten_schema: DatasetSchema) -> None:
    """Prove that the subfields of an object field get prefixed."""
    field = kadastraleobjecten_schema.get_table_by_id("kadastraleobjecten").get_field_by_id(
        "soortCultuurOnbebouwd"
    )
    assert {"soortCultuurOnbebouwdCode", "soortCultuurOnbebouwdOmschrijving"} == {
        sf.name for sf in field.subfields
    }


def test_json_subfield_does_not_crash(kadastraleobjecten_schema: DatasetSchema) -> None:
    """Prove that the subfields of an object field get prefixed."""
    field = kadastraleobjecten_schema.get_table_by_id("kadastraleobjecten").get_field_by_id(
        "soortGrootte"
    )
    assert len(field.subfields) == 0


def test_raise_exception_on_missing_properties_in_array(schema_loader):
    """Test if a human-readable error is raised on missing properties key."""
    schema = schema_loader.get_dataset_from_file("missing_properties.json")
    with pytest.raises(
        KeyError, match=r"Key 'properties' not defined in 'meetbouten.broken_array'"
    ):
        schema.get_table_by_id("meetbouten")


def test_load_publisher_object_from_dataset(schema_loader):
    """Test that we can retrieve a publisher object from a DatasetSchema
    as defined by metaschema 2.0"""
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema.publisher == {
        "name": "Datateam Harry",
        "id": "HARRY",
        "shortname": "harhar",
        "tags": {"team": "taggy", "costcenter": "123456789.4321.13519"},
    }


def test_load_scope_object_from_dataset(schema_loader):
    """Test that we can retrieve a scope object from a DatasetSchema
    as defined by metaschema 2.0"""
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema.auth == frozenset({"HARRY/ONE"})


def test_load_scope_object_from_table(schema_loader):
    """Test that we can retrieve a scope object from a DatasetSchema
    as defined by metaschema 2.0"""
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema.tables[0].auth == frozenset({"HARRY/TWO"})


def test_load_scope_object_from_field(schema_loader):
    """Test that we can retrieve a scope object from a DatasetSchema
    as defined by metaschema 2.0"""
    schema = schema_loader.get_dataset_from_file("metaschema2.json")
    field = schema.tables[0].get_field_by_id("identificatie")

    assert field.auth == frozenset({"HARRY/THREE"})


@pytest.mark.parametrize(
    "scopes,expected_fields",
    [
        (  # Test scope for Dataset access
            ["DS/AUTH"],
            {
                "t1": {"id", "schema"},
                "t2": set(),
            },
        ),
        (  # Test scope for Table access
            ["DS/AUTH", "TABLE/AUTH"],
            {
                "t1": {"id", "schema"},
                "t2": {"id", "schema"},
            },
        ),
        (  # Test scope for field access with table scope
            ["DS/AUTH", "TABLE/AUTH", "FIELD/AUTH"],
            {
                "t1": {"id", "schema", "scoped_field"},
                "t2": {"id", "schema", "scoped_field"},
            },
        ),
        (  # Test scope for field access without table scope
            ["DS/AUTH", "FIELD/AUTH"],
            {
                "t1": {"id", "schema", "scoped_field"},
                "t2": set(),
            },
        ),
    ],
)
def test_scope_filtering(schema_loader, scopes, expected_fields):
    """Test that providing a list of scopes only returns fields within those scopes"""
    schema = schema_loader.get_dataset_from_file("scope_filtering.json")

    filtered_schema = DatasetSchema.filter_on_scopes(schema, scopes)
    filtered_fields = {}
    filtered_fields["t1"] = {field.id for field in filtered_schema.tables[0].fields}
    filtered_fields["t2"] = {field.id for field in filtered_schema.tables[1].fields}

    assert filtered_fields == expected_fields


@pytest.mark.parametrize(
    "tables,expected_tables",
    [
        ([], []),
        (["meta"], ["meta"]),
        (["meta", "gemeentes", "bestaat_niet"], ["meta", "gemeentes"]),
    ],
)
def test_table_filtering(schema_loader, tables, expected_tables):
    """Test that providing a list of scopes only returns fields within those scopes"""
    schema = schema_loader.get_dataset_from_file("table_filtering.json")

    filtered_schema = DatasetSchema.filter_on_tables(schema, tables)
    filtered_tables = [t.id for t in filtered_schema.get_version("v1").tables]
    assert set(filtered_tables) == set(expected_tables)


def test_scope_and_table_filtering(schema_loader):
    """Test that providing a list of scopes only returns fields within those scopes"""
    scopes = ["DS/AUTH", "TABLE/AUTH", "FIELD/AUTH"]
    tables = ["scoped_table"]
    schema = schema_loader.get_dataset_from_file("scope_filtering.json")

    filtered_schema = DatasetSchema.filter_on_tables(
        DatasetSchema.filter_on_scopes(schema, scopes), tables
    )
    filtered_tables = [t.id for t in filtered_schema.get_version("v1").tables]
    assert filtered_tables == tables

    filtered_fields = [field.id for field in filtered_schema.tables[0].fields]

    assert set(filtered_fields) == {"id", "schema", "scoped_field"}


def test_load_multiple_scope_objects(schema_loader):
    """Test that we can retrieve a scope object from a DatasetSchema
    as defined by metaschema 2.0"""
    schema = schema_loader.get_dataset_from_file("metaschema2.json")
    field = schema.tables[0].get_field_by_id("id")

    assert field.auth == frozenset({"HARRY/ONE", "HARRY/TWO"})


scope_a = Scope.from_dict(
    {
        "id": "SCOPE/A",
        "name": "scope A",
        "owner": {"$ref": "publishers/BENK"},
        "accessPackages": {"production": "p-scope_a", "nonProduction": "ot-scope_a"},
    }
)
scope_b = Scope.from_dict(
    {
        "id": "SCOPE/B",
        "name": "scope B",
        "owner": {"$ref": "publishers/BENK"},
        "accessPackages": {"production": "p-scope_b", "nonProduction": "ot-scope_b"},
    }
)


def test_scopes_comparison():
    scope_b2 = Scope.from_dict(
        {
            "id": "SCOPE/B",
            "name": "scope B",
            "owner": {"$ref": "publishers/BENK"},
            "accessPackages": {"production": "p-scope_b", "nonProduction": "ot-scope_b"},
        }
    )

    assert scope_a != scope_b
    assert scope_b == scope_b2

    set_one = frozenset({scope_a, scope_b})
    assert set_one - {scope_b2} == frozenset({scope_a})


def test_scope_json_data():
    assert scope_a.json_data() == {
        "id": "SCOPE/A",
        "name": "scope A",
        "owner": {"$ref": "publishers/BENK"},
        "accessPackages": {"production": "p-scope_a", "nonProduction": "ot-scope_a"},
    }


def test_scope_db_python_names():
    assert scope_a.db_name == "scope_a"
    assert scope_a.python_name == "scope_a"


def test_find_scope_by_id_is_happy(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema._find_scope_by_id(HARRY_ONE_SCOPE.id) == HARRY_ONE_SCOPE


@pytest.mark.xfail(raises=ScopeNotFound)
def test_find_scope_by_id_fails_gracefully(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    schema._find_scope_by_id("SOME_RANDOM_SCOPE_ID_THAT_DOESNT_EXIST")


def test_loading_scopes_from_dataset(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema.scopes == frozenset({HARRY_ONE_SCOPE})


def test_loading_scopes_from_table(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    assert schema.tables[0].scopes == frozenset({HARRY_TWO_SCOPE})


def test_loading_scopes_from_field(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")
    field = schema.tables[0].get_field_by_id("identificatie")

    assert field.scopes == frozenset({HARRY_THREE_SCOPE})


def test_loading_multiple_scopes_from_field(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")
    field = schema.tables[0].get_field_by_id("id")

    assert field.scopes == frozenset({HARRY_ONE_SCOPE, HARRY_TWO_SCOPE})


def _assert_scopes_are_resolved(element):
    auth = element.get("auth")
    if isinstance(auth, list):
        assert all("accessPackages" in a for a in auth)
    if isinstance(auth, dict):
        assert "accessPackages" in auth
    if element.get("type") == "object":
        # nested field
        for sub_field in element["properties"].values():
            _assert_scopes_are_resolved(sub_field)


def test_schema_json_data_can_inline_scopes(schema_loader):
    schema = schema_loader.get_dataset_from_file("metaschema2.json")

    json_data = schema.json_data(inline_tables=True, inline_publishers=True, inline_scopes=True)

    _assert_scopes_are_resolved(json_data)
    _assert_scopes_are_resolved(json_data["versions"]["v1"]["tables"][0])
    _assert_scopes_are_resolved(json_data["versions"]["v1"]["tables"][0]["schema"])


def test_repr_broken_schema():
    """Regression test: __repr__ and __missing__ performed infinite mutual recursion
    when dealing with broken schemas.
    """
    try:
        DatasetTableSchema({}, parent_schema=None)
    except KeyError:  # KeyError is ok, RecursionError isn't.
        pass


def test_relation_with_extra_properties_has_through_table(gebieden_schema):
    """Prove that a 1-N relation with extra properties on the relates add a through table."""
    tables_including_through = {t.id for t in gebieden_schema.get_tables(include_through=True)}
    assert "ggwgebieden_ligtInStadsdeel" in tables_including_through


def test_extra_properties_of_relation_field_are_also_in_through_table(gebieden_schema):
    """Prove that extra fields defined on the relation are also showing up in the through table."""
    tables_including_through = {t.id: t for t in gebieden_schema.get_tables(include_through=True)}
    fields_on_through_table = {
        f.id: f for f in tables_including_through["bouwblokken_ligtInBuurt"].fields
    }
    # the field `ligtInBuurt` that is on the through table should have the fields
    # `beginGeldigheid` and `eindGeldigheid` because those are also defined on the field
    # in the source table of the relation.
    assert {"beginGeldigheid", "eindGeldigheid"} < fields_on_through_table[
        "ligtInBuurt"
    ].json_data()["properties"].keys()


def test_table_zoom(gebieden_schema: DatasetSchema):
    bouwblokken = gebieden_schema.get_table_by_id("bouwblokken")
    buurten = gebieden_schema.get_table_by_id("buurten")

    # bouwblokken has a zoom min and max set
    assert bouwblokken.min_zoom == 20
    assert bouwblokken.max_zoom == 28
    # buurten has no zoom set, and therefore uses the defaults
    assert buurten.min_zoom == 15
    assert buurten.max_zoom == 30


def test_datasetversions(schema_loader):
    """
    Test dataset versioning results in multiple versions on the dataset and
    backwards compatibility for tables still works.
    """
    dataset = schema_loader.get_dataset("metaschema3")

    assert dataset.default_version == "v1"
    assert len(dataset.versions) == 2

    # Test backwards compatible properties
    assert dataset.tables == dataset.get_tables("v1")
    assert dataset.has_an_available_version

    # Test each version of the dataset is accessible
    assert not dataset.get_version("v0").enable_api
    assert dataset.get_version("v1").enable_api


def test_row_level_auth(schema_loader):
    dataset = schema_loader.get_dataset_from_file("brp_row_level_auth.json")
    table = dataset.tables[0]  # there is only one table here.

    rla = table.rla

    assert rla.targets == [
        "verblijfplaatsHuisnummer",
        "verblijfplaatsStraatnaam",
        "verblijfplaatsPostcode",
    ]
    assert rla.source == "verblijfplaatsAfgeschermd"
    assert rla.auth_map[True] == frozenset({Scope.from_string("BRP/R-PLUS")})
    assert rla.auth_map[False] == frozenset({Scope.from_string("BRP/R")})


def test_dataset_get_diffs(schema_loader):
    """
    Test if get_diffs method extracts the correct differences
    between 2 instances of woonplaatsen dataset
    """
    base_dataset = schema_loader.get_dataset_from_file("woonplaatsen.json")
    update_dataset = schema_loader.get_dataset_from_file("woonplaatsen_diffs.json")
    assert base_dataset.get_diffs(update_dataset) == {
        "dictionary_item_added": [
            "root['versions']['v1']['tables'][0]['schema']['properties']['testAddedField']"
        ],
        "values_changed": {
            "root['versions']['v1']['tables'][0]['version']": {
                "new_value": "1.1.0",
                "old_value": "1.0.0",
            }
        },
    }


def test_dataset_get_diffs_incomp_datasets(schema_loader):
    """Comparisons between 2 different datasets should raise exception"""
    appels = schema_loader.get_dataset_from_file("woonplaatsen.json")
    peren = schema_loader.get_dataset_from_file("afval.json")

    with pytest.raises(IncompatibleDataset):
        appels.get_diffs(peren)


def test_subresource(schema_loader):
    """
    Test to see that the resource table has a subresource to the subresource table
    """
    dataset = schema_loader.get_dataset("subresources")
    resource_table = dataset.get_table_by_id("resource")
    subresource_table = dataset.get_table_by_id("subresource")

    subresources = resource_table.subresources
    assert len(subresources) == 1
    assert subresources[0].table == subresource_table
    assert subresources[0].field == "ofResource"
