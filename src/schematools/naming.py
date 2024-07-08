from __future__ import annotations

import re
from functools import lru_cache
from re import Match, Pattern
from typing import Final

from string_utils import slugify

from schematools import RELATION_INDICATOR

_RE_CAMEL_CASE: Final[Pattern[str]] = re.compile(
    r"(((?<=[^A-Z])[A-Z])|([A-Z](?![A-Z]))|((?<=[a-z])[0-9])|(?<=[0-9])[a-z]|((?<=[A-Z])[A-Z](?=[A-Z])))"  # noqa: E501
)

_CAMEL_CASE_REPLACE_PAT: Final[Pattern[str]] = re.compile(
    r"""
    (?:_|\s)+   # Find word boundaries by looking for underscore and whitespace characters, they
                # will be discarded (not captured)
    (.)         # Capture first letter of word on word boundary
    |           # OR
    (\d+)       # Capture a number
    (?:_|\s)*   # Optionally followed by underscore and whitespace characters (to be discarded)
    (.)         # Capture first letter of word on word boundary
    """,
    re.VERBOSE,
)


@lru_cache(maxsize=500)
def toCamelCase(ident: str, first_upper=False) -> str:
    """Convert an identifier to camelCase format.

    Word boundaries are determined by:
    - numbers
    - underscore characters
    - whitespace characters (this violates the concept of identifiers,
      but we handle it nevertheless)

    A camelCased identifier, when it starts with a letter, it will start with a lower cased letter.

    Empty strings are not allowed. They will raise an :exc:`ValueError` exception.

    Examples::

        >>> toCamelCase("dataset_table_schema")
        'datasetTableSchema'
        >>> toCamelCase("dataset table schema")
        'datasetTableSchema'
        >>> toCamelCase("fu_33_bar")
        'fu33Bar'
        >>> toCamelCase("fu_33bar")
        'fu33Bar'
        >>> toCamelCase("fu_33Bar")
        'fu33Bar'
        >>> toCamelCase("33_fu_bar")
        '33FuBar'

    Args:
        ident: The identifier to be converted.

    Returns:
        The identifier in camelCase format.

    Raises:
        ValueError: If ``indent`` is an empty string.

    """
    if ident == "":
        raise ValueError("Parameter `ident` cannot be an empty string.")

    def replacement(m: Match) -> str:
        # As we use the OR operator in the regular expression with capture groups on both sides,
        # we will always have at least one capture group that results in `None`. We filter those
        # out in the generator expression. Even though a captured group sometimes represents a
        # number (as a string), we still call `upper()` on it. That's faster than another
        # explicit test.
        return "".join(s.upper() for s in m.groups() if s)

    result = _CAMEL_CASE_REPLACE_PAT.sub(replacement, ident)
    if first_upper:
        return result[0].upper() + result[1:]
    else:
        # The first letter of camelCase identifier is always lower case
        return result[0].lower() + result[1:]


@lru_cache(maxsize=500)
def to_snake_case(ident: str) -> str:
    """Convert an identifier to snake_case format.

    Empty strings are not allowed. They will raise an :exc:`ValueError` exception.

    Args:
        ident: The identifier to be converted.

    Returns:
        The identifier in snake_case foramt.

    Raises:
        ValueError: If ``ident`` is an empty string.
    """
    if ident == "":
        raise ValueError("Parameter `ident` cannot be an empty string.")
    # Convert to field name, avoiding snake_case to snake_case issues.
    # Also preserve RELATION_INDICATOR in names (RELATION_INDICATOR are used for object relations)
    name_parts = [toCamelCase(part) for part in ident.split(RELATION_INDICATOR)]
    return RELATION_INDICATOR.join(
        slugify(_RE_CAMEL_CASE.sub(r" \1", part).strip(), separator="_") for part in name_parts
    )
