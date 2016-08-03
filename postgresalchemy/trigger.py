import inspect
import zlib
from sqlalchemy import Table
from sqlalchemy.sql.elements import ClauseList


class BaseTrigger(object):

    _valid_execution_times = {"BEFORE", "AFTER", "INSTEAD_OF"}
    _valid_defers = {"NOT DEFERRABLE", "DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE INITIALLY DEFERRED"}
    _valid_cardinalities = {"FOR EACH ROW", "FOR EACH STATEMENT"}

    def __call__(self, f):
        signature = inspect.signature(f)
        parameters = signature.parameters.values()
        self._function = f.__name__

    def __str__(self):
        event = " OR ".join(self._event)
        statement = self._sql_template.format(name=self._name, execution_time=self._execution_time, event=event,
                                              selectable=self._selectable, from_table=self._from_table,
                                              deferr=self._defer, cardinality=self._cardinality,
                                              condition=self._condition, function=self._function,
                                              arguments=self._arguments)
        return statement

    @property
    def after(self) -> 'TriggerEvent':
        self._execution_time = "AFTER"
        return TriggerEvent(self)

    def _set_execution_time(self, execution_time: str):
        execution_time = execution_time.upper()
        if execution_time not in self._valid_execution_times:
            valid_execution_times = " | ".join(self._valid_execution_times)
            raise ValueError("Invalid execution time argument, use one of: %s" % valid_execution_times)
        self._execution_time = execution_time

    def _set_event(self, event):
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
            cardinality = "FOR EACH ROW"
        elif cardinality.endswith("STATEMENT"):
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
        self._arguments = ",".join("'s'" % str(a) for a in arguments)


class Trigger(BaseTrigger):

    _sql_template = """
        CREATE TRIGGER {name} {execution_time} {event} on {selectable}
        {from_table}
        {defer}
        {cardinality}
        {condition}
        EXECUTE PROCEDURE {function} ({arguments})
    """

    def __init__(self, name=None, function=None, execution_time="BEFORE", event="INSERT", selectable=None,
                 from_table=None, defer="NOT DEFERRABLE", cardinality="ROW", condition='', arguments=None):
        self._function = function
        self._set_execution_time(execution_time)
        self._set_event(event)
        self._set_selectable(selectable)
        self._set_from_table(from_table)
        self._set_defer(defer)
        self._set_cardinality(cardinality)
        self._set_condition(condition)
        self._set_arguments(arguments)
        if not name:
            self._name = "placeholder"

    @property
    def before(self) -> 'TriggerEvent':
        self._execution_time = "BEFORE"
        return TriggerEvent(self)

    @property
    def instead_of(self) -> 'TriggerEvent':
        self._execution_time = "INSTEAD OF"
        return TriggerEvent(self)


class ConstraintTrigger(BaseTrigger):
    def __init__(self, name=None, function=None, event="INSERT", selectable=None,
                 from_table=None, defer="NOT DEFERRABLE", cardinality="ROW", condition=None, arguments=None):
        self._function = function
        self._set_execution_time("AFTER")
        self._set_event(event)
        self._set_selectable(selectable)
        self._set_from_table(from_table)
        self._set_defer(defer)
        self._set_cardinality(cardinality)
        self._set_condition(condition)
        self._set_arguments(arguments)
        if not name:
            self._name = "placeholder"


class TriggerClause(object):
    def __call__(self, f):
        return self._trigger(f)


class TriggerEvent(TriggerClause):
    def __init__(self, trigger: Trigger, event=None):
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

    @property
    def truncate(self) -> 'TriggerEvent':
        self._trigger._set_event("TRUNCATE")
        return self

    def on(self, selectable):
        self._trigger._set_selectable(selectable)
        return self


class TriggerArguments(TriggerClause):
    def __init__(self, trigger, arguments=None):
        self._trigger = trigger
        if arguments:
            self._trigger._set_arguments(arguments)

    def with_arguments(self, arguments):
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


class TriggerCardinalityConditions(TriggerClause):
    def __init__(self, trigger, cardinality=None):
        self._trigger = trigger
        if cardinality:
            self._trigger._cardinality = cardinality

    @property
    def row(self) -> TriggerCondition:
        self._trigger._cardinality = "FOR EACH ROW"
        return TriggerCondition(self._trigger)

    @property
    def statement(self) -> TriggerCondition:
        self._trigger._cardinality = "STATEMENT"
        return TriggerCondition(self._trigger)


class TriggerCardinality(TriggerCondition):
    def __init__(self, trigger):
        self._trigger = trigger

    @property
    def for_each(self) -> TriggerCardinalityConditions:
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