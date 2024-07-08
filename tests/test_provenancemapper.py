from __future__ import annotations

from schematools.importer.base import Provenance


def test_row_plain():
    """Prove that a regular dict just work as is."""
    value = Provenance("colname1").resolve({"colname1": 12, "colname2": "test"})
    assert value == 12


def test_row_with_jsonpath_provenance():
    """Prove that a provenance based on json path works."""
    value = Provenance("$.colname2.sub").resolve({"colname1": 12, "colname2": {"sub": "test"}})
    assert value == "test"
