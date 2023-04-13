from django.db import models


class UnlimitedCharField(models.CharField):
    # When we switch to Django 4.2, replace this with CharField(max_length=None).
    description = "CharField without length limit"
    max_length = (1 << 31) - 1  # Big limit to fool validation code.

    def __init__(self, *args, **kwargs):
        if "max_length" in kwargs:
            raise Exception("max_length not supported, use an ordinary CharField")
        super().__init__(*args, **kwargs)

    def db_type(self, connection):
        return "varchar"
