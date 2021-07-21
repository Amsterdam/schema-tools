from schematools.types import DatasetSchema, Permission, PermissionLevel


def test_permission_level_ordering() -> None:
    """Test whether enum ordering works based on the int values."""
    assert sorted(PermissionLevel._member_map_.values()) == [
        PermissionLevel.none,
        PermissionLevel.subobjects_only,
        PermissionLevel.letters,
        PermissionLevel.random,
        PermissionLevel.encoded,
        PermissionLevel.read,
        PermissionLevel.highest,  # alias for read
    ]
    assert PermissionLevel.highest is PermissionLevel.read
    assert PermissionLevel.highest is max(PermissionLevel._member_map_.values())


def test_geo_and_id_when_configured(here, gebieden_schema):
    schema = DatasetSchema.from_file(here / "files" / "meetbouten.json")
    table = schema.get_table_by_id("meetbouten")
    assert table.identifier == ["nummer"]
    assert table.main_geometry == "geometrie"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_geo_and_id_when_not_configured(here):
    schema = DatasetSchema.from_file(here / "files" / "afvalwegingen.json")
    table = schema.get_table_by_id("containers")
    assert table.identifier == ["id"]
    assert table.main_geometry == "geometry"
    id_field = [field for field in table.fields if [field.name] == table.identifier][0]
    assert id_field.is_primary


def test_import_dataset_separate_table_files(here):
    """Prove that datasets with tables in separate files are created correctly."""
    schema = DatasetSchema.from_file(here / "files" / "gebieden_sep_tables" / "dataset.json")
    assert len(schema.tables) == 2
    table = schema.get_table_by_id("buurten")
    assert table.main_geometry == "primaireGeometrie"


def test_profile_schema(brp_r_profile_schema):
    """Prove that the profile files are properly read,
    and have their fields access the JSON data.
    """
    assert brp_r_profile_schema.scopes == {"BRP/R"}

    brp = brp_r_profile_schema.datasets["brp"]
    table = brp.tables["ingeschrevenpersonen"]

    assert table.permissions.level is PermissionLevel.read
    assert table.fields["bsn"] == Permission(PermissionLevel.encoded)
    assert table.mandatory_filtersets == [
        ["bsn", "lastname"],
        ["postcode", "lastname"],
    ]


def test_fetching_of_related_schema_ids(here):
    """Prove that ids of related dataset schemas are properly collected."""
    schema = DatasetSchema.from_file(here / "files" / "multirelation.json")
    assert set(schema.related_dataset_schema_ids) == {"gebieden", "meetbouten"}
