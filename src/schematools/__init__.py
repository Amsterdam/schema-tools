"""Schematools, a library to work with amsterdam schema files."""

from typing import Final

RELATION_INDICATOR: Final[str] = "_"
MAX_TABLE_NAME_LENGTH: Final[int] = 63  # limitation of PostgreSQL
TMP_TABLE_POSTFIX: Final[str] = "_new"
TABLE_INDEX_POSTFIX: Final[str] = "_idx"
DATABASE_SCHEMA_NAME_DEFAULT: Final[str] = "public"
DEFAULT_SCHEMA_URL: Final[str] = "https://schemas.data.amsterdam.nl/datasets/"
DEFAULT_PROFILE_URL: Final[str] = "https://schemas.data.amsterdam.nl/profiles/"
