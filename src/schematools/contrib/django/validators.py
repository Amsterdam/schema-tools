from django.core.validators import RegexValidator
from django.utils.deconstruct import deconstructible
from django.utils.translation import gettext_lazy as _


@deconstructible
class URLPathValidator(RegexValidator):
    regex = r"\A[a-z0-9]+([/-][a-z0-9]+)*\Z"
    message = _("Only these characters are allowed: a-z, 0-9, '-' and '/' between paths.")
