from __future__ import annotations

import pytest

from schematools.naming import to_snake_case, toCamelCase


def test_toCamelCase() -> None:
    """Confirm that:
    - space separated name is converted to camelCase
    - PascalCase results in camelCase
    - snake_case results in camelCase
    """
    assert toCamelCase("test name magic") == "testNameMagic"
    assert toCamelCase("test name magic2") == "testNameMagic2"
    assert toCamelCase("testNameMagic") == "testNameMagic"
    assert toCamelCase("TestNameMagic") == "testNameMagic"
    assert toCamelCase("test_name_magic") == "testNameMagic"
    assert toCamelCase("numbers_33_in_the_middle_44") == "numbers33InTheMiddle44"
    # mind the lower case "i" after "33". It should be made upper case
    assert toCamelCase("numbers33inTheMiddle44") == "numbers33InTheMiddle44"
    assert toCamelCase("per_jaar_per_m2") == "perJaarPerM2"
    assert toCamelCase("eerste_h_n_id") == "eersteHNId"

    with pytest.raises(ValueError):
        toCamelCase("")


def test_toCamelCase_first_upper() -> None:
    """Confirm that first_upper=True upper cases the first letter (PascalCase).

    This is the form used to build class names from dataset and table ids
    (see schematools.types).
    """
    assert toCamelCase("dataset_table_schema", first_upper=True) == "DatasetTableSchema"
    assert toCamelCase("test name magic", first_upper=True) == "TestNameMagic"
    assert toCamelCase("testNameMagic", first_upper=True) == "TestNameMagic"
    assert toCamelCase("TestNameMagic", first_upper=True) == "TestNameMagic"
    assert toCamelCase("per_jaar_per_m2", first_upper=True) == "PerJaarPerM2"
    assert toCamelCase("eerste_h_n_id", first_upper=True) == "EersteHNId"
    # a leading number has no letter to upper case, so it is left unchanged
    assert toCamelCase("33_fu_bar", first_upper=True) == "33FuBar"

    with pytest.raises(ValueError):
        toCamelCase("", first_upper=True)


def test_to_snake_case() -> None:
    """Confirm that:
    - space separated name converted to snake_case
    - camelCase converted to snake_case
    - snake_case converted to snake_case
    """
    assert to_snake_case("test name magic") == "test_name_magic"
    assert to_snake_case("test name magic22") == "test_name_magic_22"
    assert to_snake_case("TestNameMagic") == "test_name_magic"
    assert to_snake_case("testNameMagic") == "test_name_magic"
    assert to_snake_case("test_name_magic") == "test_name_magic"
    assert to_snake_case("hoofdroutes_u_routes") == "hoofdroutes_u_routes"
    assert to_snake_case("verlengingSluitingstijd1") == "verlenging_sluitingstijd_1"
    assert to_snake_case("numbers33inTheMiddle44") == "numbers_33_in_the_middle_44"
    assert to_snake_case("perJaarPerM2") == "per_jaar_per_m2"
    assert to_snake_case("eersteHNId") == "eerste_h_n_id"

    with pytest.raises(ValueError):
        to_snake_case("")


def test_snake_and_camel_case_functions_are_inverses() -> None:
    """Prove that snake and camel case functions are inverses of eachother."""
    # mind the lower case "i" after "33". It should be made upper case
    assert to_snake_case(toCamelCase("per_jaar_per_m2")) == "per_jaar_per_m2"
    assert to_snake_case(toCamelCase("test_name_magic")) == "test_name_magic"
    assert (
        to_snake_case(toCamelCase("numbers_33_in_the_middle_44")) == "numbers_33_in_the_middle_44"
    )
    assert to_snake_case(toCamelCase("eerste_h_n_id")) == "eerste_h_n_id"
    assert toCamelCase(to_snake_case("numbers33InTheMiddle44")) == "numbers33InTheMiddle44"

    assert toCamelCase(to_snake_case("testNameMagic")) == "testNameMagic"
    assert toCamelCase(to_snake_case("testNameMagic2")) == "testNameMagic2"
    assert toCamelCase(to_snake_case("eersteHNId")) == "eersteHNId"
