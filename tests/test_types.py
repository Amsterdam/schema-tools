from schematools.types import DatasetSchema


def test_geo_and_id_when_configured(here):
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
