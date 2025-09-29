from __future__ import annotations

import pytest
from django.core.management import call_command

from schematools.contrib.django import models


@pytest.mark.django_db
def test_import_scopes(here):
    path = here / "files/scopes/GLEBZ/glebzscope.json"
    args = [path]
    call_command("import_scopes", *args)
    assert models.Scope.objects.count() == 1
    assert models.Scope.objects.first().name == "GLEBZ"
