import inspect
from typing import Union, Sequence

from abc import ABC, abstractmethod
from .util import get_condition_text, get_name
from .types import FluentClauseContainer, DependentCreatable


class TriggerClause(object):
    def __init__(self, trigger: 'Trigger'):
        self._trigger = trigger
        self._trigger._current_clause = self

    def __call__(self, f):
        return self._trigger(f)


class TriggerEvent(TriggerClause):
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
        column_names = ", ".join(get_name(c) for c in columns)
        self._trigger._set_event("UPDATE OF %s" % column_names)
        return self

    @property
    def truncate(self) -> 'TriggerEvent':
        self._trigger._set_event("TRUNCATE")
        return self

    def on(self, selectable) -> 'TriggerFrom':
        self._trigger._set_selectable(selectable)
        return TriggerFrom(self._trigger)


class BaseTrigger(FluentClauseContainer, DependentCreatable):
    _valid_execution_times = {"BEFORE", "AFTER", "INSTEAD OF"}
    _valid_defers = {"NOT DEFERRABLE", "DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE INITIALLY DEFERRED"}
    _valid_cardinalities = {"FOR EACH ROW", "FOR EACH STATEMENT"}
    _sql_create_template = """
        CREATE {constraint} TRIGGER {name} {execution_time} {event} on {selectable}
        {from_table}
        {defer}
        {cardinality}
        {condition}
        EXECUTE PROCEDURE {function} ({arguments})
    """

    _sql_drop_template = """
        DROP TRIGGER IF EXISTS {name} on {selectable}
    """

    def __init__(self, name, function=None, execution_time="AFTER", event="INSERT", selectable='',
                 from_table='', defer="NOT DEFERRABLE", cardinality="ROW", condition='', arguments=''):
        self._execution_time = None
        self._function = None
        self._event = []
        self._selectable = None
        self._from_table = None
        self._defer = None
        self._cardinality = None
        self._condition = None
        self._arguments = None
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

    @property
    def _create_statement(self):
        if not self._function:
            raise RuntimeError("No function has been specified for this trigger to execute")
        event = " OR ".join(self._event)
        arguments = ",".join("'s'" % str(a) for a in self._arguments)
        selectable = get_name(self._selectable) if self._selectable else ''
        from_table = get_name(self._from_table) if self._from_table else ''
        statement = self._sql_create_template.format(name=self._name, constraint=self._constraint,
                                                     execution_time=self._execution_time, event=event,
                                                     selectable=selectable, from_table=from_table,
                                                     deferr=self._defer, cardinality=self._cardinality,
                                                     condition=self._condition, function=self._function,
                                                     arguments=arguments)
        return statement

    @property
    def _drop_statement(self):
        selectable = get_name(self._selectable) if self._selectable else ''
        statement = self._sql_drop_template.format(name=self._name, selectable=selectable)
        return statement

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

    def _set_execution_time(self, execution_time: str):
        execution_time = execution_time.upper()
        if execution_time not in self._valid_execution_times:
            valid_execution_times = " | ".join(self._valid_execution_times)
            raise ValueError("Invalid execution time argument, use one of: %s" % valid_execution_times)
        self._execution_time = execution_time

    def _set_event(self, event):
        if self._execution_time == "INSTEAD OF" and event == "TRUNCATE":
            raise ValueError("Triggers do not support 'INSTEAD OF' timing for 'TRUNCATE' events")
        elif isinstance(event, str):
            if event not in self._event:
                self._event.append(event)
        elif isinstance(event, Sequence):
            self._event = list(event)

    def _set_selectable(self, selectable):
        if hasattr(selectable, "__table__"):
            selectable = selectable.__table__
        self._selectable = selectable

    def _set_from_table(self, from_table):
        if hasattr(from_table, "__table__"):
            from_table = from_table.__table__
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
        self._condition = get_condition_text(condition)

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
    def with_arguments(self, *arguments):
        self._trigger._set_arguments(arguments)
        return self.__call__


class TriggerCondition(TriggerArguments):
    def when(self, condition) -> TriggerArguments:
        self._trigger._set_condition(condition)
        return TriggerArguments(self._trigger)


class TriggerRestrictedCardinalityConditions(TriggerClause):
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
    @property
    def for_each(self) -> CardinalityConditions:
        if "TRUNCATE" in self._trigger._event:
            return TriggerRestrictedCardinalityConditions(self._trigger)
        else:
            return TriggerCardinalityConditions(self._trigger)


class TriggerDeferrableConditions(TriggerClause):
    @property
    def immediate(self) -> TriggerCardinality:
        self._trigger._set_defer("DEFERRABLE INITIALLY IMMEDIATE")
        return TriggerCardinality(self._trigger)

    @property
    def deferred(self) -> TriggerCardinality:
        self._trigger._set_defer("DEFERRABLE INITIALLY DEFERRED")
        return TriggerCardinality(self._trigger)


class TriggerDeferrable(TriggerCardinality):
    @property
    def deferrable(self) -> TriggerDeferrableConditions:
        return TriggerDeferrableConditions(self._trigger)

    def not_deferrable(self) -> TriggerCardinality:
        self._trigger._set_defer("NOT DEFERRABLE")
        return TriggerCardinality(self._trigger)


class TriggerFrom(TriggerDeferrable):
    def from_table(self, from_table) -> TriggerDeferrable:
        self._trigger._set_from_table(from_table)
        return TriggerDeferrable(self._trigger)
