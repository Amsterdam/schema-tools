from __future__ import annotations

import json

from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.utils.deconstruct import deconstructible
from django.utils.translation import gettext_lazy as _


@deconstructible
class URLPathValidator(RegexValidator):
    regex = r"\A[a-z0-9]+([/-][a-z0-9]+)*\Z"
    message = _("Only these characters are allowed: a-z, 0-9, '-' and '/' between paths.")


def validate_json(value: str) -> None:
    """Validate the contents of a JSON value."""
    try:
        json.loads(value)
    except (TypeError, ValueError):
        raise ValidationError(
            _("Value must be valid JSON text."),
            code="invalid",
            params={"value": value},
        ) from None
