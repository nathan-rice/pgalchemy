import inspect
import typing
from datetime import date, time, datetime, timedelta
from collections import OrderedDict
import sqlalchemy

import re


Array = type('Array', typing.Sequence.__bases__, dict(typing.Sequence.__dict__))





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

