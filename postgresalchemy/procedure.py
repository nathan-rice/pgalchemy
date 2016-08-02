import inspect
import zlib
import typing
import sqlalchemy
import re

from .types import *


class Procedure(object):
    _sql_template = """
        CREATE FUNCTION {name} ({parameters}) RETURNS {return_type} $$
        {code}
        $$ LANGUAGE plpython3u;
    """

    def __init__(self, name=None, parameters="", return_type="void", code=""):
        if not name:
            self.name = "procedure_" + str(abs(zlib.adler32(code)))
        self.parameters = parameters
        self.return_type = return_type
        self.code = code

    def __str__(self):
        return self._sql_template.format(name=self.name, parameters=self.parameters, return_type=self.return_type,
                                         code=self.code)


class ProcedureGenerator(object):
    _re_flags = re.DOTALL | re.MULTILINE
    _function_body_re = re.compile(r"\s*def\s+[^(]+\(.*?\)\s*(?:->\s*[^\n]+)?\s*:(?:\s*#[^\n]*)?\n(.*)",
                                   flags=_re_flags)

    @classmethod
    def from_function(cls, f):
        parameters = cls.get_parameters(f)
        sql_parameters = ", ".join(cls.convert_python_type_to_sql(p) for p in parameters)
        sql_return = cls.generate_return_type(f)
        # Code related
        function_body = cls.get_function_body(f)
        cls.check_for_overwritten_input_parameters(function_body)
        return Procedure(name=f.__name__, parameters=sql_parameters, return_type=sql_return, code=function_body)

    @staticmethod
    def get_parameters(f):
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
        if isinstance(python_type, sqlalchemy.Table):
            type_name = python_type.name
        elif hasattr(python_type, "__table__"):
            type_name = python_type.__table__.name
        elif issubclass(python_type, Array):
            type_name = cls.convert_python_type_to_sql(python_type.__args__[0]) + "[]"
        else:
            type_name = mappings.get(python_type)
            if not type_name:
                raise ValueError("No Postgres mapping was found for Python type: %s" % python_type)
        return type_name

    @classmethod
    def generate_sql_default_value(cls, type_name, default=None):
        if default is not None:
            default_parameters = type_name, cls.convert_python_value_to_sql(default)
            return " DEFAULT ".join(default_parameters)
        else:
            return type_name

    @staticmethod
    def convert_python_value_to_sql(value):
        def convert_inner(val):
            if val is None:
                return "NULL"
            elif isinstance(val, (str, int, float, bool)):
                return str(val)
            elif isinstance(val, (datetime, date, time)):
                return val.isoformat()
            elif isinstance(val, Sequence):
                array_values = ', '.join(convert_inner(v) for v in val)
                return "{%s}" % array_values
        result = "'%s'" % convert_inner(value)
        return result

    @classmethod
    def generate_sql_procedure_parameter(cls, parameter):
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
