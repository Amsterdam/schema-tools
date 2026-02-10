from __future__ import annotations

import pytest
from django.db import connection

from schematools.contrib.django.management.commands.create_views import (
    _clean_sql,
    _execute_multi_sql,
)


@pytest.mark.django_db
class TestCreateViews:
    def test_execute_multi_sql_executes_multiple_statements(self) -> None:
        view_sql = """
            CREATE TABLE element (
                id int,
                name varchar(255)
            );
            INSERT INTO element (id, name) VALUES (1, 'name');
            SELECT * FROM element;
        """
        with connection.cursor() as cursor:
            sql = _clean_sql(view_sql)
            _execute_multi_sql(cursor, sql)
            result = cursor.fetchall()
            assert result == [(1, "name")]

    def test_execute_multi_sql_executes_single_statement(self, afvalwegingen_dataset) -> None:
        view_sql = "SELECT name FROM datasets_dataset;"
        with connection.cursor() as cursor:
            sql = _clean_sql(view_sql)
            _execute_multi_sql(cursor, sql)
            result = cursor.fetchall()
            assert result == [("afvalwegingen",)]
