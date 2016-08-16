from typing import Sequence


class PostgresOption(object):
    """Base class for Postgres command options."""


class FluentClauseContainer(object):
    _current_clause = None

    def __getattr__(self, attr):
        if hasattr(type(self._current_clause), attr):
            return getattr(self._current_clause, attr)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, attr))


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

