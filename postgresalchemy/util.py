from sqlalchemy import Table, Column
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.orm.attributes import InstrumentedAttribute
from .role import get_role_name


def get_column_name(c):
    if isinstance(c, Column):
        name = "%s.%s" % (c.table.name, c.name)
    elif isinstance(c, InstrumentedAttribute):
        column = c.prop.columns[0]  # Need the underlying database table column
        name = "%s.%s" % (column.table.name, column.name)
    else:
        name = c
    return name


def get_table_name(t):
    if hasattr(t, "__table__"):
        name = t.__table__.name
    elif isinstance(t, Table):
        name = t.name
    else:
        name = t
    return name


def get_condition_text(condition):
    if isinstance(condition, ClauseList):
        condition = condition.compile(compile_kwargs={"literal_binds": True})
    return condition

