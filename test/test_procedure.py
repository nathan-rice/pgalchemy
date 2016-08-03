import pytest
from .config import *


def test_get_function_body_simple():
    body = p.ProcedureGenerator.get_function_body(example_1)
    assert body.endswith("return True\n")


def test_get_function_body_with_parameter():
    body = p.ProcedureGenerator.get_function_body(example_2)
    assert body.endswith("return True\n")


def test_get_function_body_with_return_annotation():
    body = p.ProcedureGenerator.get_function_body(example_3)
    assert body.endswith("return True\n")


def test_get_function_body_with_multiline_parameter():
    body = p.ProcedureGenerator.get_function_body(example_4)
    assert body.endswith("return True\n")


def test_get_function_body_with_multiline_parameter_and_multiline_body():
    body = p.ProcedureGenerator.get_function_body(example_5)
    body_lines = body.strip().split("\n")
    assert len(body_lines) == 3
    assert body_lines[0].endswith("a = 2")
    assert body_lines[1].endswith("b = False")
    assert body_lines[2].endswith("return c")


def test_check_for_overwritten_input_parameters_should_pass():
    body = p.ProcedureGenerator.get_function_body(example_4)
    parameters = p.ProcedureGenerator.get_parameters(example_4)
    p.ProcedureGenerator.check_for_overwritten_input_parameters(parameters, body)
    assert True


def test_check_for_overwritten_input_parameters_with_exception():
    body = p.ProcedureGenerator.get_function_body(example_5)
    parameters = p.ProcedureGenerator.get_parameters(example_5)
    with pytest.raises(ValueError):
        p.ProcedureGenerator.check_for_overwritten_input_parameters(parameters, body)


def test_convert_python_type_to_sql_simple():
    assert p.ProcedureGenerator.convert_python_type_to_sql(bool) == "boolean"
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.datetime) == "timestamp without time zone"


def test_convert_python_type_to_sql_string_passthrough():
    assert p.ProcedureGenerator.convert_python_type_to_sql("boolean") == "boolean"


def test_convert_python_type_to_sql_array():
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.Array[bool]) == "boolean[]"


def test_convert_python_type_to_sql_2d_array():
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.Array[p.Array[bool]]) == "boolean[][]"


def test_convert_python_type_to_sql_sqlalchemy_class():
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.Array[TestMappedClass]) == "test_table[]"


def test_convert_python_type_to_sql_notfound():
    with pytest.raises(ValueError):
        p.ProcedureGenerator.convert_python_type_to_sql(p.Sequence)


def test_generate_return_type_void():
    assert p.ProcedureGenerator.generate_return_type(example_1) == "void"


def test_generate_return_type_simple():
    assert p.ProcedureGenerator.generate_return_type(example_3) == "boolean"


def test_generate_return_type_sequence():
    assert p.ProcedureGenerator.generate_return_type(example_5) == "SETOF int"


def test_convert_value_to_sql_none():
    assert p.ProcedureGenerator.convert_python_value_to_sql(None) == "NULL"


def test_convert_value_to_sql_boolean():
    assert p.ProcedureGenerator.convert_python_value_to_sql(True) == "True"


def test_convert_value_to_sql_boolean():
    assert p.ProcedureGenerator.convert_python_value_to_sql(1.0) == "1.0"


def test_convert_value_to_sql_datetime():
    now = p.datetime.utcnow()
    date_string = p.ProcedureGenerator.convert_python_value_to_sql(now)
    assert date_string == "'%s'" % now.isoformat()


def test_convert_value_to_sql_array():
    my_array = [1, 2, 3]
    array_string = p.ProcedureGenerator.convert_python_value_to_sql(my_array)
    assert array_string == "ARRAY[1,2,3]"


def test_convert_value_to_sql_array_2d():
    my_array = [[1, 2], [3, 4]]
    array_string = p.ProcedureGenerator.convert_python_value_to_sql(my_array)
    assert array_string == "ARRAY[ARRAY[1,2],ARRAY[3,4]]"


def test_convert_python_value_to_sql_unknown():
    with pytest.raises(ValueError):
        p.ProcedureGenerator.convert_python_value_to_sql(complex)


def test_generate_sql_default_value_nodefault():
    type_and_default = p.ProcedureGenerator.generate_sql_default_value("boolean")
    assert type_and_default == "boolean"


def test_generate_sql_default_value_boolean_default():
    type_and_default = p.ProcedureGenerator.generate_sql_default_value("boolean", True)
    assert type_and_default == "boolean DEFAULT True"


def test_generate_sql_default_value_null_default():
    type_and_default = p.ProcedureGenerator.generate_sql_default_value("boolean", None)
    assert type_and_default == "boolean DEFAULT NULL"


def test_generate_sql_default_value_float_default():
    type_and_default = p.ProcedureGenerator.generate_sql_default_value("float", 1.0)
    assert type_and_default == "float DEFAULT 1.0"


def test_generate_procedure_from_function():
    procedure = p.ProcedureGenerator.from_function(example_4)
    assert procedure.name == "example_4"
    assert procedure.parameters == ["a int DEFAULT 1", "b boolean DEFAULT True", "c int[] DEFAULT ARRAY[1,2,3]"]
    assert procedure.return_type == "boolean"


if __name__ == "__main__":
    pass
    # pytest.main()
