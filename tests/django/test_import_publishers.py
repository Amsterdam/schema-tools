from __future__ import annotations

import pytest
from django.core.management import call_command

from schematools.contrib.django import models


@pytest.mark.django_db
def test_import_publishers(here):
    path = here / "files/publishers/GLEBZ.json"
    args = [path]
    call_command("import_publishers", *args)
    assert models.Publisher.objects.count() == 1
    assert models.Publisher.objects.first().id == "GLEBZ"
