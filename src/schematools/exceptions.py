class ParserError(ValueError):
    """Exception to indicate the parsing failed."""


class SchemaObjectNotFound(ValueError):
    """Field does not exist."""


class ViewObjectNotFound(ValueError):
    """SQL view does not exist."""


class DatasetNotFound(SchemaObjectNotFound):
    """The dataset could not be found."""


class DatasetTableNotFound(SchemaObjectNotFound):
    """The table could not be found."""


class DatasetFieldNotFound(SchemaObjectNotFound):
    """The field could not be found."""


class IncompatibleMetaschema(Exception):
    """This package version of schema-tools
    is being used with a metaschema that it is
    not compatible with."""


class PendingMetaschemaDeprecation(PendingDeprecationWarning):
    """The used metaschema is marked for deprecation."""
