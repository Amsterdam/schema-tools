from __future__ import annotations


class ParserError(ValueError):
    """Exception to indicate the parsing failed."""


class SchemaObjectNotFound(ValueError):
    """Field does not exist."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class ViewObjectNotFound(ValueError):
    """SQL view does not exist."""


class DatasetNotFound(SchemaObjectNotFound):
    """The dataset could not be found."""


class DatasetVersionNotFound(SchemaObjectNotFound):
    """The version could not be found."""


class DatasetTableNotFound(SchemaObjectNotFound):
    """The table could not be found."""


class DatasetFieldNotFound(SchemaObjectNotFound):
    """The field could not be found."""


class IncompatibleMetaschema(Exception):
    """This package version of schema-tools
    is being used with a metaschema that it is
    not compatible with."""


class IncompatibleDataset(Exception):
    """Datasets to compare should be the same. Cannot compare different datasets."""


class PendingMetaschemaDeprecation(PendingDeprecationWarning):
    """The used metaschema is marked for deprecation."""


class DuplicateScopeId(ValueError):
    """The ID is already used in another scope."""


class DuplicateProfileId(ValueError):
    """The ID is already used for another profile."""


class ScopeNotFound(SchemaObjectNotFound):
    """The scope could not be found."""


class LoaderNotFound(Exception):
    """The loader could not be found."""
