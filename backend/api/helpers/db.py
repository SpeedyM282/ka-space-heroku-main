import logging

from django.db import connection

logger = logging.getLogger(__name__)


def fetch_raw_sql(sql, params=None, as_dict=True):
    params = params or []
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        logger.debug(cursor.query.decode())
        if as_dict:
            return dict_fetchall(cursor)
        else:
            return cursor.fetchall()


def dict_fetchall(cursor):
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def execute_sql(sql, params=None):
    params = params or []
    with connection.cursor() as cursor:
        result = cursor.execute(sql, params)
        logger.debug(cursor.query.decode())
        return result
