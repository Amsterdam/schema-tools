class ParserError(ValueError):
    """Exception to indicate the parsing failed."""


class SchemaObjectNotFound(ValueError):
    """Field does not exist."""


class DatasetNotFound(ValueError):
    """The dataset could not be found."""
