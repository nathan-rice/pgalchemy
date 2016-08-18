import re
from typing import Sequence
from datetime import date, datetime, time
from sqlalchemy import Table, Column
from sqlalchemy.engine import Connection, Dialect, Engine
from sqlalchemy.event import listen
from sqlalchemy.dialects.postgres import dialect
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.orm.attributes import InstrumentedAttribute


def before_create(statement, table=Table):
    def handle_event(target, connection, **kwargs):
        execute_if_postgres(connection, statement)
    listen(table, "before_create", handle_event)


def after_create(statement, table=Table):
    def handle_event(target, connection, **kwargs):
        execute_if_postgres(connection, statement)
    listen(table, "after_create", handle_event)


def before_drop(statement, table=Table):
    def handle_event(target, connection, **kwargs):
        execute_if_postgres(connection, statement)
    listen(table, "before_drop", handle_event)


def after_drop(statement, table=Table):
    def handle_event(target, connection, **kwargs):
        execute_if_postgres(connection, statement)
    listen(table, "after_drop", handle_event)


def get_name(e):
    if isinstance(e, Column):
        name = "%s.%s" % (e.table.name, e.name)
    elif isinstance(e, InstrumentedAttribute):
        column = e.prop.columns[0]  # Need the underlying database table column
        name = "%s.%s" % (column.table.name, column.name)
    elif hasattr(e, "__table__"):
        name = e.__table__.name
    elif hasattr(e, "name"):
        name = e.name
    else:
        name = e
    return name


def get_condition_text(condition):
    if isinstance(condition, ClauseList):
        condition = condition.compile(compile_kwargs={"literal_binds": True})
    return condition


def is_postgres(connection_dialect_or_engine):
    if isinstance(connection_dialect_or_engine, Connection):
        dialect_is_postgres = isinstance(connection_dialect_or_engine.engine.dialect, dialect)
    elif isinstance(connection_dialect_or_engine, Dialect):
        dialect_is_postgres = isinstance(connection_dialect_or_engine, dialect)
    elif isinstance(connection_dialect_or_engine, Engine):
        dialect_is_postgres = isinstance(connection_dialect_or_engine.dialect, dialect)
    else:
        argument_type = type(connection_dialect_or_engine)
        raise ValueError("Expected either a Connection, Dialect or Engine, got a %s" % argument_type)
    return dialect_is_postgres


def execute_if_postgres(connection, statement):
    if is_postgres(connection):
        try:
            connection.execute(statement)
        except Exception:
            pass


def convert_python_value_to_sql(value):
    def convert_inner(val):
        if val is None:
            converted_val = "NULL"
        elif isinstance(val, str):
            converted_val = "'%s'" % val.replace("'", "''")
        elif isinstance(val, (int, float, bool)):
            converted_val = str(val)
        elif isinstance(val, (datetime, date, time)):
            converted_val = "'%s'" % val.isoformat()
        elif isinstance(val, Sequence):
            array_values = ','.join(convert_inner(v) for v in val)
            converted_val = "ARRAY[%s]" % array_values
        elif hasattr(val, "__table__"):
            converted_val = val.__table__.name
        else:
            raise ValueError("No known type mapping for Python type: %s" % type(val))
        return converted_val

    result = convert_inner(value)
    return result


_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')


def camelcase_to_underscore(name):
    s1 = _first_cap_re.sub(r'\1_\2', name)
    return _all_cap_re.sub(r'\1_\2', s1).lower()
