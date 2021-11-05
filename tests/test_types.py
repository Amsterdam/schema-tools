from pathlib import Path

import pytest

from schematools.types import DatasetSchema, Permission, PermissionLevel, SemVer
from schematools.utils import dataset_schema_from_path


def test_permission_level_ordering() -> None:
    """Test whether enum ordering works based on the int values."""
    assert sorted(PermissionLevel._member_map_.values()) == [
        PermissionLevel.NONE,
        PermissionLevel.SUBOBJECTS_ONLY,
        PermissionLevel.LETTERS,
        PermissionLevel.RANDOM,
        PermissionLevel.ENCODED,
        PermissionLevel.READ,
        PermissionLevel.highest,  # alias for read
    ]
    assert PermissionLevel.highest is PermissionLevel.READ
    assert PermissionLevel.highest is max(PermissionLevel._member_map_.values())


def test_geo_and_id_when_configured(here, gebieden_schema):
    schema = dataset_schema_from_path(here / "files" / "meetbouten.json")
    table = schema.get_table_by_id("meetbouten")
    assert table.identifier == ["identificatie"]
    assert table.main_geometry == "geometrie"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_geo_and_id_when_not_configured(here):
    schema = dataset_schema_from_path(here / "files" / "afvalwegingen.json")
    table = schema.get_table_by_id("containers")
    assert table.identifier == ["id"]
    assert table.main_geometry == "geometry"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_import_dataset_separate_table_files(here):
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


def test_profile_schema(brp_r_profile_schema):
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


def test_fetching_of_related_schema_ids(here):
    """Prove that ids of related dataset schemas are properly collected."""
    schema = dataset_schema_from_path(here / "files" / "multirelation.json")
    assert set(schema.related_dataset_schema_ids) == {"gebieden", "meetbouten"}


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
    assert str(SemVer("1.2.3")) == "v1.2.3"
    assert str(SemVer("1.2")) == "v1.2.0"
    assert str(SemVer("1")) == "v1.0.0"

    assert str(SemVer("v1.2.3")) == "v1.2.3"
    assert str(SemVer("v1.2")) == "v1.2.0"
    assert str(SemVer("v1")) == "v1.0.0"


def test_semver_repr() -> None:
    """Test SemVer repr represenation."""
    assert repr(SemVer("1.2.3")) == 'SemVer("v1.2.3")'
    assert repr(SemVer("1.2")) == 'SemVer("v1.2.0")'
    assert repr(SemVer("1")) == 'SemVer("v1.0.0")'

    assert repr(SemVer("v1.2.3")) == 'SemVer("v1.2.3")'
    assert repr(SemVer("v1.2")) == 'SemVer("v1.2.0")'
    assert repr(SemVer("v1")) == 'SemVer("v1.0.0")'


def test_semver_eq() -> None:
    """Test SemVer __eq__."""
    assert SemVer("1.2.3") == SemVer("v1.2.3")
    assert SemVer("1.2.0") == SemVer("v1.2")
    assert SemVer("1.0.0") == SemVer("v1")


def test_semver_lt() -> None:
    """Test SemVer __lt__."""
    assert SemVer("1.2.3") < SemVer("v1.2.4")
    assert SemVer("1.2") < SemVer("v1.2.4")
    assert SemVer("1") < SemVer("v1.2.4")

    assert SemVer("94.1.0") < SemVer("95.1.0")

    assert SemVer("94.1.0") < SemVer("94.2.0")


def test_dataset_schema_get_fields_with_surrogate_pk(
    compound_key_schema: DatasetSchema, verblijfsobjecten_schema: DatasetSchema
):
    """Prove that the surrogate 'id' key is returned once for schemas with a
    compound key, regardless of whether the surrogate key is already defined
    by the schema or generated"""

    verblijfsobjecten = verblijfsobjecten_schema.tables[0]
    compound_key_schema = compound_key_schema.tables[0]

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
    assert sorted([x.name for x in compound_key_schema.get_fields(include_subfields=False)]) == [
        "beginGeldigheid",
        "eindGeldigheid",
        "id",
        "identificatie",
        "schema",
        "volgnummer",
    ]
