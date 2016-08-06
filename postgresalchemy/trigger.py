import inspect
from typing import Union
import zlib
from sqlalchemy import Table, Column
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ClauseList
from abc import ABC, abstractmethod
from .util import get_column_name


class TriggerClause(object):
    def __call__(self, f):
        return self._trigger(f)


class TriggerEvent(TriggerClause):
    def __init__(self, trigger: 'Trigger', event=None):
        self._trigger = trigger
        if event:
            self._trigger._set_event(event)

    @property
    def insert(self) -> 'TriggerEvent':
        self._trigger._set_event("INSERT")
        return self

    @property
    def delete(self) -> 'TriggerEvent':
        self._trigger._set_event("DELETE")
        return self

    @property
    def update(self) -> 'TriggerEvent':
        self._trigger._set_event("UPDATE")
        return self

    def update_of(self, *columns) -> 'TriggerEvent':
        column_names = ", ".join(get_column_name(c) for c in columns)
        self._trigger._set_event("UPDATE OF %s" % column_names)
        return self

    @property
    def truncate(self) -> 'TriggerEvent':
        self._trigger._set_event("TRUNCATE")
        return self

    def on(self, selectable) -> 'TriggerFrom':
        self._trigger._set_selectable(selectable)
        return TriggerFrom(self._trigger)


class BaseTrigger(ABC):
    _valid_execution_times = {"BEFORE", "AFTER", "INSTEAD OF"}
    _valid_defers = {"NOT DEFERRABLE", "DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE INITIALLY DEFERRED"}
    _valid_cardinalities = {"FOR EACH ROW", "FOR EACH STATEMENT"}
    _sql_template = """
        CREATE {constraint} TRIGGER {name} {execution_time} {event} on {selectable}
        {from_table}
        {defer}
        {cardinality}
        {condition}
        EXECUTE PROCEDURE {function} ({arguments})
    """

    def __init__(self, name=None, function=None, execution_time="AFTER", event="INSERT", selectable='',
                 from_table='', defer="NOT DEFERRABLE", cardinality="ROW", condition='', arguments=''):
        self._function = function
        self._set_execution_time(execution_time)
        self._set_event(event)
        self._set_selectable(selectable)
        self._set_from_table(from_table)
        self._set_defer(defer)
        self._set_cardinality(cardinality)
        self._set_condition(condition)
        self._set_arguments(arguments)
        self._set_constraint()
        self._name = name

    def __call__(self, f):
        self._set_function(f)
        return f

    def __str__(self):
        if not self._function:
            raise RuntimeError("No function has been specified for this trigger to execute")
        name = self._get_name()
        event = " OR ".join(self._event)
        arguments = ",".join("'s'" % str(a) for a in self._arguments)
        statement = self._sql_template.format(name=name, constraint=self._constraint,
                                              execution_time=self._execution_time, event=event,
                                              selectable=self._selectable, from_table=self._from_table,
                                              deferr=self._defer, cardinality=self._cardinality,
                                              condition=self._condition, function=self._function,
                                              arguments=arguments)
        return statement

    def _get_name(self):
        return self._name or self._generate_name()

    def _set_function(self, f):
        if f:
            signature = inspect.signature(f)
            required_args = len(list(p for p in signature.parameters.values() if p.default == inspect._empty))
            supplied_args = len(self._arguments)
            if not supplied_args >= required_args:
                message = "Number of supplied arguments (%s) does not match the number required by the function (%s)"
                raise ValueError(message % (supplied_args, required_args))
            if not signature.return_annotation == Trigger:
                raise ValueError("Functions specified in triggers must have a return type annotation of Trigger")
            f = f.__name__
        self._function = f

    @abstractmethod
    def _set_constraint(self):
        return NotImplemented

    def _generate_name(self):
        if not self._function:
            raise ValueError
        event = ','.join(self._event)
        parameters = (self._execution_time, event, self._selectable, self._from_table, self._defer, self._cardinality,
                      self._condition, self._function, self._arguments)
        return zlib.adler32(''.join(parameters))

    def _set_execution_time(self, execution_time: str):
        execution_time = execution_time.upper()
        if execution_time not in self._valid_execution_times:
            valid_execution_times = " | ".join(self._valid_execution_times)
            raise ValueError("Invalid execution time argument, use one of: %s" % valid_execution_times)
        self._execution_time = execution_time

    def _set_event(self, event):
        if self._execution_time == "INSTEAD OF" and event == "TRUNCATE":
            raise ValueError("Triggers do not support 'INSTEAD OF' timing for 'TRUNCATE' events")
        if not hasattr(self, "_event"):
            self._event = []
        if event not in self._event:
            self._event.append(event)

    def _set_selectable(self, selectable):
        if isinstance(selectable, Table):
            selectable = selectable.name
        elif hasattr(selectable, "__table__"):
            selectable = selectable.__table__.name
        self._selectable = selectable

    def _set_from_table(self, from_table):
        if isinstance(from_table, Table):
            from_table = from_table.name
        elif hasattr(from_table, "__table__"):
            from_table = from_table.__table__.name
        self._from_table = from_table

    def _set_defer(self, defer: str):
        defer = defer.upper()
        if defer.endswith("IMMEDIATE"):
            defer = "DEFERRABLE INITIALLY IMMEDIATE"
        elif defer.endswith("DEFERRED"):
            defer = "DEFERRABLE INITIALLY DEFERRED"
        elif not defer == "NOT DEFERRABLE":
            valid_defers = " | ".join(self._valid_defers)
            raise ValueError("Invalid defer argument, use one of: %s" % valid_defers)
        self._defer = defer

    def _set_cardinality(self, cardinality: str):
        cardinality = cardinality.upper()
        if cardinality.endswith("ROW"):
            if "TRUNCATE" in self._event:
                raise ValueError("'FOR EACH ROW' is not supported for 'TRUNCATE' triggers")
            cardinality = "FOR EACH ROW"
        elif cardinality.endswith("STATEMENT"):
            if self._execution_time == "INSTEAD OF":
                raise ValueError("'FOR EACH STATEMENT' is not supported for 'INSTEAD OF' triggers")
            cardinality = "FOR EACH STATEMENT"
        else:
            valid_cardinalities = " | ".join(self._valid_cardinalities)
            raise ValueError("Invalid cardinality argument, use one of: %s" % valid_cardinalities)
        self._cardinality = cardinality

    def _set_condition(self, condition):
        if isinstance(condition, ClauseList):
            condition = condition.compile(compile_kwargs={"literal_binds": True})
        self._condition = condition

    def _set_arguments(self, arguments):
        self._arguments = arguments


class Trigger(BaseTrigger):
    def _set_constraint(self):
        self._constraint = ''

    @property
    def after(self) -> TriggerEvent:
        self._set_execution_time("AFTER")
        return TriggerEvent(self)

    @property
    def before(self) -> TriggerEvent:
        self._set_execution_time("BEFORE")
        return TriggerEvent(self)

    @property
    def instead_of(self) -> TriggerEvent:
        self._set_execution_time("INSTEAD OF")
        return TriggerEvent(self)


class ConstraintTrigger(BaseTrigger, TriggerEvent):
    _valid_execution_times = {"AFTER"}

    def _set_constraint(self):
        self._constraint = "CONSTRAINT"
        self._trigger = self  # Required for inheritance from TriggerEvent to function properly


class TriggerArguments(TriggerClause):
    def __init__(self, trigger, arguments=None):
        self._trigger = trigger
        if arguments:
            self._trigger._set_arguments(arguments)

    def with_arguments(self, *arguments):
        self._trigger._set_arguments(arguments)
        return self.__call__


class TriggerCondition(TriggerArguments):
    def __init__(self, trigger, condition=None):
        self._trigger = trigger
        if condition:
            self._trigger._set_condition(condition)

    def when(self, condition) -> TriggerArguments:
        self._trigger._set_condition(condition)
        return TriggerArguments(self._trigger)


class TriggerRestrictedCardinalityConditions(TriggerClause):
    def __init__(self, trigger, cardinality=None):
        self._trigger = trigger
        if cardinality:
            self._trigger._set_cardinality(cardinality)

    @property
    def statement(self) -> TriggerCondition:
        self._trigger._set_cardinality("STATEMENT")
        return TriggerCondition(self._trigger)


class TriggerCardinalityConditions(TriggerRestrictedCardinalityConditions):
    @property
    def row(self) -> TriggerCondition:
        self._trigger._set_cardinality("FOR EACH ROW")
        return TriggerCondition(self._trigger)


CardinalityConditions = Union[TriggerRestrictedCardinalityConditions, TriggerCardinalityConditions]


class TriggerCardinality(TriggerCondition):
    def __init__(self, trigger):
        self._trigger = trigger

    @property
    def for_each(self) -> CardinalityConditions:
        if "TRUNCATE" in self._trigger._event:
            return TriggerRestrictedCardinalityConditions(self._trigger)
        else:
            return TriggerCardinalityConditions(self._trigger)


class TriggerDeferrableConditions(TriggerClause):
    def __init__(self, trigger):
        self._trigger = trigger

    @property
    def immediate(self) -> TriggerCardinality:
        self._trigger._set_defer("DEFERRABLE INITIALLY IMMEDIATE")
        return TriggerCardinality(self._trigger)

    @property
    def deferred(self) -> TriggerCardinality:
        self._trigger._set_defer("DEFERRABLE INITIALLY DEFERRED")
        return TriggerCardinality(self._trigger)


class TriggerDeferrable(TriggerCardinality):
    def __init__(self, trigger):
        self._trigger = trigger

    @property
    def deferrable(self) -> TriggerDeferrableConditions:
        return TriggerDeferrableConditions(self._trigger)

    def not_deferrable(self) -> TriggerCardinality:
        self._trigger._set_defer("NOT DEFERRABLE")
        return TriggerCardinality(self._trigger)


class TriggerFrom(TriggerDeferrable):
    def __init__(self, trigger, from_table=None):
        self._trigger = trigger
        if from_table:
            self._trigger._set_from_table(from_table)

    def from_table(self, from_table) -> TriggerDeferrable:
        self._trigger._set_from_table(from_table)
        return TriggerDeferrable(self._trigger)
