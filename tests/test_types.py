import operator
from functools import partial
from pathlib import Path

import pytest

from schematools.exceptions import SchemaObjectNotFound
from schematools.types import (
    DatasetSchema,
    Json,
    Permission,
    PermissionLevel,
    ProfileSchema,
    SemVer,
    TableVersions,
)
from schematools.utils import dataset_schema_from_path


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


def test_geo_and_id_when_configured(here: Path, gebieden_schema: DatasetSchema) -> None:
    schema = dataset_schema_from_path(here / "files" / "meetbouten.json")
    table = schema.get_table_by_id("meetbouten")
    assert table.identifier == ["identificatie"]
    assert table.main_geometry == "geometrie"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_geo_and_id_when_not_configured(here: Path) -> None:
    schema = dataset_schema_from_path(here / "files" / "afvalwegingen.json")
    table = schema.get_table_by_id("containers")
    assert table.identifier == ["id"]
    assert table.main_geometry == "geometry"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_import_dataset_separate_table_files(here: Path) -> None:
    """Prove that datasets with tables in separate files are created correctly."""
    schema = dataset_schema_from_path(here / "files" / "gebieden_sep_tables" / "dataset.json")
    assert len(schema.tables) == 2
    table = schema.get_table_by_id("buurten")
    assert table.main_geometry == "primaireGeometrie"


def test_datasetschema_from_file_not_a_dataset(here: Path) -> None:
    """Ensure a proper exception is raised when loading a file that's not a DatasetSchema."""

    error_msg = "Invalid Amsterdam Dataset schema file"
    with pytest.raises(ValueError, match=error_msg):
        # v1.0.0.json is a DatasetRow, not a DatasetSchema.
        dataset_schema_from_path(
            here / "files" / "gebieden_sep_tables" / "bouwblokken" / "v1.0.0.json"
        )

    with pytest.raises(ValueError, match=error_msg):
        # not_a_json_file.txt is not a JSON file. We should still get our ValueError.
        dataset_schema_from_path(here / "files" / "not_a_json_file.txt")


def test_profile_schema(brp_r_profile_schema: ProfileSchema) -> None:
    """Prove that the profile files are properly read,
    and have their fields access the JSON data.
    """
    assert brp_r_profile_schema.scopes == {"BRP/R"}

    brp = brp_r_profile_schema.datasets["brp"]
    table = brp.tables["ingeschrevenpersonen"]

    assert table.permissions.level is PermissionLevel.READ
    assert table.fields["bsn"] == Permission(PermissionLevel.ENCODED)
    assert table.mandatory_filtersets == [
        ["bsn", "lastname"],
        ["postcode", "lastname"],
    ]


def test_fetching_of_related_schema_ids(here: Path) -> None:
    """Prove that ids of related dataset schemas are properly collected."""
    schema = dataset_schema_from_path(here / "files" / "multirelation.json")
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
    assert sorted([x.id for x in verblijfsobjecten.get_fields(include_subfields=False)]) == [
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
    assert sorted([x.name for x in composite_key_schema.get_fields(include_subfields=False)]) == [
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


def test_dataset_with_camel_cased_id_generates_correct_through_relations(here):
    """Prove that the relation identifiers in through tables are correctly generated.

    When a through table is generated, two FK relations are inserted in this table,
    one to the source and one to the target table.
    When dataset.id or table.id in the source or target table are camelCased,
    the generated identifiers should stay intact, and should not be snakecased.
    """
    schema = dataset_schema_from_path(here / "files" / "multirelation.json")
    through_table = schema.through_tables[0]

    # The fields `hasrelations` and `hasNMRelation` are the FK's to source and target table.
    assert through_table.get_field_by_id("hasrelations").relation == "baseDataset:hasrelations"
    assert through_table.get_field_by_id("hasNMRelation").relation == "baseDataset:internalRelated"


def test_from_dict(afval_schema_json: Json) -> None:
    is_dict = lambda o: isinstance(o, dict)
    is_tv = lambda o: isinstance(o, TableVersions)

    assert all(map(is_dict, afval_schema_json["tables"]))

    dataset = DatasetSchema.from_dict(afval_schema_json)

    # Internally `DatasetSchema` uses `TableVersions` instances as the elements of the "tables"
    # list.
    assert all(map(is_dict, afval_schema_json["tables"]))

    # However that should not affect the dict that was originally passed in to `from_dict`
    assert all(map(is_tv, dataset["tables"]))


def test_subfields(ggwgebieden_schema: DatasetSchema) -> None:
    field = ggwgebieden_schema.get_table_by_id("buurten").get_field_by_id("ligtInWijk")
    subfields = sorted(field.subfields, key=operator.attrgetter("id"))
    assert subfields[0].id == "identificatie"
    assert subfields[1].id == "volgnummer"

    assert subfields[0] == field.get_field_by_id("identificatie")
    assert subfields[1] == field.get_field_by_id("volgnummer")
    with pytest.raises(SchemaObjectNotFound):
        field.get_field_by_id("iDoNotExist")


def test_raise_exception_on_missing_properties_in_array(here):
    """Test if a human-readable error is raised on missing properties key."""
    schema = dataset_schema_from_path(here / "files" / "missing_properties.json")
    with pytest.raises(
            KeyError,
            match=r"Key 'properties' not defined in 'meetbouten.broken_array'"
    ):
        schema.get_table_by_id("meetbouten")
