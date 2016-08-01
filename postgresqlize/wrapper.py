import inspect
import typing
from datetime import date, time, datetime, timedelta
from collections import OrderedDict
import sqlalchemy
from sqlalchemy.sql.elements import ClauseList
import re


Array = type('Array', typing.Sequence.__bases__, dict(typing.Sequence.__dict__))


class TriggerClause(object):
    def __call__(self, f):
        return self._trigger(f)


class Trigger(object):

    _valid_defers = {"NOT DEFERRABLE", "DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE INITIALLY DEFERRED"}
    _valid_cardinalities = {"FOR EACH ROW", "FOR EACH STATEMENT"}

    def __init__(self, name="trigger", execution_time="BEFORE", event="INSERT", selectable=None, from_table=None,
                 defer="NOT DEFERRABLE", cardinality="ROW", condition=None, arguments=None):
        self._name = name
        self._execution_time = execution_time
        self._set_event(event)
        self._set_selectable(selectable)
        self._set_from_table(from_table)
        self._set_defer(defer)
        self._cardinality = cardinality
        self._set_condition(condition)
        self._set_arguments(arguments)

    def __call__(self, f):
        pass

    @property
    def before(self) -> 'TriggerEvent':
        self._execution_time = "BEFORE"
        return TriggerEvent(self)

    @property
    def after(self) -> 'TriggerEvent':
        self._execution_time = "AFTER"
        return TriggerEvent(self)

    @property
    def instead_of(self) -> 'TriggerEvent':
        self._execution_time = "INSTEAD OF"
        return TriggerEvent(self)

    def _set_event(self, event):
        if not hasattr(self, "_event"):
            self._event = []
        if event not in self._event:
            self._event.append(event)

    def _set_selectable(self, selectable):
        if isinstance(selectable, sqlalchemy.Table):
            selectable = selectable.name
        elif hasattr(selectable, "__table__"):
            selectable = selectable.__table__.name
        self._selectable = selectable

    def _set_from_table(self, from_table):
        if isinstance(from_table, sqlalchemy.Table):
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
            raise ValueError("Invalid defer argument, use one of: %s" % " | ".join(self._valid_defers))
        self._defer = defer

    def _set_cardinality(self, cardinality: str):
        cardinality = cardinality.upper()
        if cardinality.endswith("ROW"):
            cardinality = "FOR EACH ROW"
        elif cardinality.endswith("STATEMENT"):
            cardinality = "FOR EACH STATEMENT"
        else:
            raise ValueError("Invalid cardinality argument, use one of: %s" % " | ".join(self._valid_cardinalities))

    def _set_condition(self, condition):
        if isinstance(condition, ClauseList):
            condition = condition.compile(compile_kwargs={"literal_binds": True})
        self._condition = condition

    def _set_arguments(self, arguments):
        self._arguments = ", ".join(str(a) for a in arguments)


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


class PostgresAlchemy(object):

    default_mappings = {
        bool: 'boolean',
        int: 'int',
        float: 'numeric',
        bytes: 'bytea',
        bytearray: 'bytea',
        str: 'text',
        date: 'date',
        time: 'time without time zone',
        datetime: 'timestamp without time zone',
        timedelta: 'interval'
    }

    block_begin_re = re.compile(r".*:\s*$")

    def __init__(self, engine, input_mappings=None, output_mappings=None):
        if output_mappings is None:
            output_mappings = {}
        if input_mappings is None:
            input_mappings = {}
        self.engine = engine
        self.functions = OrderedDict()
        self.input_mappings = self.default_mappings.copy()
        self.input_mappings.update(input_mappings)
        self.output_mappings = self.default_mappings.copy()
        self.output_mappings.update(output_mappings)

    def procedure(self, f):
        # Signature related
        signature = inspect.signature(f)
        parameters = signature.parameters.values()
        sql_parameters = ", ".join(self.convert_python_type_to_sql(p) for p in parameters)
        sql_return = self.generate_return_type(signature)
        # Code related
        function_source = inspect.getsourcelines(f)
        function_body = self.get_function_body(function_source)
        self.check_for_overwritten_input_parameters(function_body)
        sql_template = """
            CREATE FUNCTION {name} ({parameters}) RETURNS {return_type} $$
            {code}
            $$ LANGUAGE plpython3u;
        """.format(name=f.__name__, parameters=sql_parameters, return_type=sql_return, code=function_body)
        self.functions[f.__name__] = sql_template
        return f

    def trigger(self):
        pass

    @classmethod
    def get_function_body(cls, code_lines):
        in_body = False
        body_lines = []
        for line in code_lines:
            if not in_body:
                if cls.block_begin_re.match(line):
                    in_body = True
            else:
                body_lines.append(line)
        return body_lines

    @staticmethod
    def check_for_overwritten_input_parameters(parameters, code_lines):
        parameter_names = "|".join(p.name for p in parameters)
        parameter_overwritten_re = re.compile(r"(%s)\s*=[^=]" % parameter_names)
        for line in code_lines:
            match = parameter_overwritten_re.match(line)
            if match:
                message = "Function parameter '%s' incorrectly overwritten in function body"
                raise ValueError(message % match.group(1))

    def convert_python_type_to_sql(self, python_type, is_output=False):
        mappings = self.output_mappings if is_output else self.input_mappings
        type_name = self.get_type_mapping(python_type, mappings)
        return type_name

    def get_type_mapping(self, python_type, mappings=None):
        if not mappings:
            mappings = self.input_mappings

        if isinstance(python_type, sqlalchemy.Table):
            type_name = python_type.name
        elif hasattr(python_type, "__table__"):
            type_name = python_type.__table__.name
        elif isinstance(python_type, Array):
            type_name = self.get_type_mapping(python_type.__args__[0]) + "[]"
        else:
            type_name = mappings.get(python_type)
            if not type_name:
                raise ValueError("No Postgres mapping was found for Python type: %s" % python_type)

        return type_name

    @staticmethod
    def generate_sql_default_value(type_name, default=None):
        if default is not None:
            default_parameters = type_name, str(default)
            return " DEFAULT ".join(default_parameters)
        else:
            return type_name

    def generate_sql_procedure_parameter(self, parameter):
        type_name = self.convert_python_type_to_sql(parameter.annotation)
        sql_parameter = self.generate_sql_default_value(type_name, parameter.default)
        return sql_parameter

    def generate_return_type(self, signature):
        annotation = signature.return_annotation

        if annotation == inspect.Signature.empty:
            return_type = "void"
        elif isinstance(annotation, typing.Sequence):
            python_type = annotation.__args__[0]
            return_type = "SETOF " + self.convert_python_type_to_sql(python_type)
        else:
            return_type = self.convert_python_type_to_sql(annotation)

        return return_type

