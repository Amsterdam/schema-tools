from schematools.utils import to_snake_case, toCamelCase


def test_toCamelCase():
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


def test_to_snake_case():
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
    assert to_snake_case("verlengingSluitingstijd1") == "verlenging_sluitingstijd_1"

