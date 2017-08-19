from typing import Sequence


class PostgresOption(object):
    """Base class for Postgres command options."""


class FluentClauseContainer(object):
    _current_clause = None

    def __getattr__(self, attr):
        if hasattr(type(self._current_clause), attr):
            return getattr(self._current_clause, attr)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, attr))


class Creatable(object):

    def _create(self, connection):
        statement = self._create_statement
        if connection:
            connection.execute(statement)

    def _drop(self, connection):
        statement = self._drop_statement
        if connection:
            connection.execute(statement)

    @property
    def _create_statement(self):
        raise NotImplemented("No create statement property configured")

    @property
    def _drop_statement(self):
        raise NotImplemented("No drop statement property configured")


class DependentCreatable(Creatable):
    pass


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
