"""This module contains converters to the CKAN format.

CKAN (https://ckan.org) is the system used by the metadata catalog at
https://data.overheid.nl.
"""

from __future__ import annotations

from ._convert import from_dataset

__all__ = ["from_dataset"]
