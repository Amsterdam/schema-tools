from pathlib import Path

import pytest

from schematools.types import DatasetSchema, Permission, PermissionLevel
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
