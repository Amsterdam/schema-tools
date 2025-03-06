"""Schematools, a library to work with amsterdam schema files."""

from __future__ import annotations

from typing import Final

# Internal conventions
RELATION_INDICATOR: Final[str] = "_"
MAX_TABLE_NAME_LENGTH: Final[int] = 63  # limitation of PostgreSQL
TMP_TABLE_POSTFIX: Final[str] = "_new"
TABLE_INDEX_POSTFIX: Final[str] = "_idx"
DATABASE_SCHEMA_NAME_DEFAULT: Final[str] = "public"

# Defaults
DEFAULT_SCHEMA_URL: Final[str] = "https://schemas.data.amsterdam.nl/datasets/"
DEFAULT_PROFILE_URL: Final[str] = "https://schemas.data.amsterdam.nl/profiles/"
# The directory where all publisher objects are defined for amsterdam-schema
PUBLISHER_DIR: Final[str] = "publishers"
# Files that can exist in publishers directory but should be ignored by
# the FileLoaders
PUBLISHER_EXCLUDE_FILES: Final[list[str]] = ["publishers.json", "index.json"]
# The directory where all scope objects are defined for amsterdam-schema
SCOPE_DIR: Final[str] = "scopes"

# Common coordinate reference systems
CRS_WGS84: Final[str] = "EPSG:4326"  # World Geodetic System 1984, used in GPS
CRS_RD_NEW: Final[str] = "EPSG:28992"  # Amersfoort / RD New
SRID_RD_NEW: Final[int] = 28992

# Some likely used 3D coordinate reference systems:
SRID_3D: Final[list[int]] = [
    7415,  # Amersfoort / RD New + NAP height
    7423,  # ETRS89 + EVRF2007 height
    9286,  # ETRS89 + NAP height
    4979,  # WGS84 + height
    4978,  # WGS84 + geocentric height
]
# The meta-schema major versions in amsterdam-schema that this
# package version is compatible with. This means that it
# can only handle schema objects that are compliant
# under at least one of these versions.
COMPATIBLE_METASCHEMAS = [1, 2, 3]
