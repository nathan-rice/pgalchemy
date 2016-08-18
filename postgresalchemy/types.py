from typing import Sequence
from .util import execute_if_postgres, before_create, after_create, before_drop, after_drop

class PostgresOption(object):
    """Base class for Postgres command options."""


class FluentClauseContainer(object):
    _current_clause = None

    def __getattr__(self, attr):
        if hasattr(type(self._current_clause), attr):
            return getattr(self._current_clause, attr)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, attr))


class Creatable(object):
    _create_f = before_create
    _drop_f = after_drop

    def _create(self, connection):
        statement = self._create_statement
        if connection:
            execute_if_postgres(connection, statement)
        else:
            self._create_f(statement)

    def _drop(self, connection):
        statement = self._drop_statement
        if connection:
            execute_if_postgres(connection, statement)
        else:
            self._drop_f(statement)

    @property
    def _create_statement(self):
        raise NotImplemented("No create statement property configured")

    @property
    def _drop_statement(self):
        raise NotImplemented("No drop statement property configured")


class DependentCreatable(Creatable):
    _create_f = after_create
    _drop_f = before_drop


class ValueSetter(object):
    @staticmethod
    def set(container: list, value):
        if value:
            if isinstance(value, (str, PostgresOption)):
                if value not in container:
                    container.append(value)
            elif isinstance(value, Sequence):
                container.clear()
                container.extend(list(value))

