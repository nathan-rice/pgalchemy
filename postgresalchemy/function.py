import inspect
import zlib
import math
import re

from typing import Sequence
from datetime import date, time, datetime, timedelta
from .trigger import Trigger

Array = type('Array', Sequence.__bases__, dict(Sequence.__dict__))

mappings = {
    bool: 'boolean',
    int: 'int',
    float: 'numeric',
    bytes: 'bytea',
    bytearray: 'bytea',
    str: 'text',
    date: 'date',
    time: 'time without time zone',
    datetime: 'timestamp without time zone',
    timedelta: 'interval',
    Trigger: 'trigger'
}


class Function(object):
    _sql_template = """
        CREATE FUNCTION {name} ({parameters}) RETURNS {return_type} AS $$
        {code}
        $$ LANGUAGE plpython3u {volatile}
    """

    def __init__(self, name=None, parameters=None, return_type="void", code="", volatile=True):
        self.name = name if name is not None else "procedure_" + str(abs(zlib.adler32(code)))
        self.parameters = parameters if parameters is not None else []
        self.return_type = return_type
        self.code = code
        self.volatile = volatile

    def __str__(self):
        parameters = ", ".join(self.parameters)
        volatile = "VOLATILE" if self.volatile else "STABLE"
        return self._sql_template.format(name=self.name, parameters=parameters, return_type=self.return_type,
                                         code=self.code, volatile=volatile)


class FunctionGenerator(object):
    _re_flags = re.DOTALL | re.MULTILINE
    _function_body_re = re.compile(r"\s*def\s+[^(]+\(.*?\)\s*(?:->\s*[^\n]+)?\s*:(?:\s*#[^\n]*)?\n(.*)",
                                   flags=_re_flags)

    @classmethod
    def from_function(cls, f):
        parameters = cls.get_parameters(f)
        sql_parameters = [cls.generate_sql_function_parameter(p) for p in parameters]
        sql_return = cls.generate_return_type(f)
        # Code related
        function_body = cls.get_function_body(f)
        cls.check_for_overwritten_input_parameters(parameters, function_body)
        return Function(name=f.__name__, parameters=sql_parameters, return_type=sql_return, code=function_body)

    @staticmethod
    def get_parameters(f) -> Sequence[inspect.Parameter]:
        signature = inspect.signature(f)
        parameters = signature.parameters.values()
        return parameters

    @classmethod
    def get_function_body(cls, f):
        source = inspect.getsource(f)
        match = cls._function_body_re.match(source)
        return match.group(1)

    @classmethod
    def check_for_overwritten_input_parameters(cls, parameters, code):
        parameter_names = "|".join(p.name for p in parameters)
        parameter_overwritten_re = re.compile(r"(%s)\s*=[^=]" % parameter_names, cls._re_flags)
        match = parameter_overwritten_re.search(code)
        if match:
            message = "Function parameter '%s' incorrectly overwritten in function body"
            raise ValueError(message % match.group(1))

    @classmethod
    def convert_python_type_to_sql(cls, python_type):
        is_class = inspect.isclass(python_type)
        if hasattr(python_type, "__table__"):
            type_name = python_type.__table__.name
        elif isinstance(python_type, str):
            type_name = python_type
        elif is_class and issubclass(python_type, Array):
            type_name = cls.convert_python_type_to_sql(python_type.__args__[0]) + "[]"
        else:
            type_name = mappings.get(python_type)
            if not type_name:
                raise ValueError("No Postgres mapping was found for Python type: %s" % python_type)
        return type_name

    @classmethod
    def generate_sql_default_value(cls, type_name, default=float('nan')):
        # Using NaN here instead of None since None is a default value you might actually use
        if not (isinstance(default, float) and math.isnan(default)):
            default_parameters = type_name, cls.convert_python_value_to_sql(default)
            return " DEFAULT ".join(default_parameters)
        else:
            return type_name

    @staticmethod
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

    @classmethod
    def generate_sql_function_parameter(cls, parameter):
        type_name = cls.convert_python_type_to_sql(parameter.annotation)
        type_and_default = cls.generate_sql_default_value(type_name, parameter.default)
        sql_parameter = " ".join((parameter.name, type_and_default))
        return sql_parameter

    @classmethod
    def generate_return_type(cls, f):
        signature = inspect.signature(f)
        annotation = signature.return_annotation

        if annotation == inspect.Signature.empty:
            return_type = "void"
        elif issubclass(annotation, Sequence):
            python_type = annotation.__args__[0]
            return_type = "SETOF " + cls.convert_python_type_to_sql(python_type)
        else:
            return_type = cls.convert_python_type_to_sql(annotation)

        return return_type
