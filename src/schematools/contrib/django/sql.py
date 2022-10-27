from typing import List

from django.db import connection
from django.db.models import sql
from django.db.models.query_utils import Q
from django.db.models.sql.constants import GET_ITERATOR_CHUNK_SIZE

from schematools.contrib.django.models import DynamicModel


def get_sql_for(objects: List[DynamicModel]) -> None:
    """Get the SQL insert statements for the provided model objects."""
    # We need a real cursor here, so that `cursor.mogrify`
    # knows exactly how to render the query.
    cursor = connection.cursor()
    for obj in objects:
        values = obj._meta.local_fields
        query = sql.InsertQuery(obj)
        query.insert_values(values, [obj])
        compiler = query.get_compiler("default")
        statements = compiler.as_sql()
        for statement, params in statements:
            yield cursor.mogrify(statement, params).decode() + ";"


def get_bulk_update_sql_for(objects: List[DynamicModel], field_names: List[str]) -> None:
    """Get the SQL update statements for the provided model objects."""
    cursor = connection.cursor()
    for obj in objects:
        values = {field_name: getattr(obj, field_name) for field_name in field_names}
        query = sql.UpdateQuery(obj)
        query.add_update_values(values)
        query.where = query.where_class()
        # query.add_q(Q(pk__in=pk_list[offset : offset + GET_ITERATOR_CHUNK_SIZE]))
        query.add_q(Q(pk=obj.pk))
        compiler = query.get_compiler("default")
        statement, params = compiler.as_sql()
        yield cursor.mogrify(statement, params).decode() + ";"
