from __future__ import annotations

import pytest
from django.core.management import call_command
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

    def test_create_views_can_create_and_refresh_materialized_view(
        self, aardgasverbruik_dataset
    ) -> None:
        view_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS public.aardgasverbruik_aardgasverbruik AS
        SELECT id
        FROM public.aardgasverbruik_mra_liander_v1;

        REFRESH MATERIALIZED VIEW public.aardgasverbruik_aardgasverbruik;
        """
        aardgasverbruik_dataset.view_data = view_sql
        aardgasverbruik_dataset.save()
        aardgasverbruik_dataset.refresh_from_db()
        # First create tables.
        call_command("create_tables")

        # Create a materialized view with multiple statements. Should not fail.
        call_command("create_views")

        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM public.aardgasverbruik_aardgasverbruik;")
            assert cursor.fetchone() == (0,)
