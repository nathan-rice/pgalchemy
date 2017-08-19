import re
from typing import Sequence
from datetime import date, datetime, time
try:
    from sqlalchemy import Column
    from sqlalchemy.sql.elements import ClauseList
    from sqlalchemy.orm.attributes import InstrumentedAttribute
except ImportError:
    class _Stub(object): pass
    Column = ClauseList = InstrumentedAttribute = _Stub


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
        name = sanitize_name(e)
    return name


def get_condition_text(condition):
    if isinstance(condition, ClauseList):
        condition = condition.compile(compile_kwargs={"literal_binds": True})
    return condition


def sanitize_name(name):
    return name.replace('"', '""')


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
