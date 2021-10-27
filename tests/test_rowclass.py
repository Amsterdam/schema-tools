from schematools.importer.base import Row


def test_row_plain():
    """Prove that a regular dict just work as is."""
    row = Row({"colname1": 12, "colname2": "test"})
    assert row["colname1"] == 12
    assert row["colname2"] == "test"


def test_row_with_simple_provenance():
    """Prove that a simple provenance mapping a fieldname to another fieldname works."""
    row = Row({"colname1": 12, "colname2": "test"}, fields_provenances={"colname1": "provColname"})
    assert row["provColname"] == 12
    assert row["colname2"] == "test"


def test_row_with_jsonpath_provenance():
    """Prove that a provenance based on json path works."""
    row = Row(
        {"colname1": 12, "colname2": {"sub": "test"}},
        fields_provenances={"$.colname2.sub": "colname2"},
    )
    assert row["colname1"] == 12
    assert row["colname2"] == "test"


def test_row_with_id_special_casing():
    """Prove that the special casing for the id field works."""
    row = Row(
        {"id": 12},
        fields_provenances={"id": "neuronId"},
    )
    row["id"] = "012.3"
    assert row["neuronId"] == 12
    assert row["id"] == "012.3"
