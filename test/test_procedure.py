import typing
import pytest
import postgresalchemy.procedure as p


def example_1():
    return True


def example_2(a: int = 1):
    return True


def example_3(a: int = 1) -> bool:
    return True


def example_4(a: int = 1, b: bool = True,
              c: typing.Sequence[int] = (1, 2, 3)) -> bool:  # test
    return True


def example_5(a: int = 1, b: bool = True,
              c: typing.Sequence[int] = (1, 2, 3)) -> typing.Sequence[int]:  # test
    a = 2
    b = False
    return c


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
    parameters = p.ProcedureGenerator.get_parameters(example_5)
    p.ProcedureGenerator.check_for_overwritten_input_parameters(parameters, body)
    assert True


def test_check_for_overwritten_input_parameters_should_pass():
    body = p.ProcedureGenerator.get_function_body(example_5)
    parameters = p.ProcedureGenerator.get_parameters(example_5)
    with pytest.raises(ValueError):
        p.ProcedureGenerator.check_for_overwritten_input_parameters(parameters, body)


def test_convert_python_type_to_sql_simple():
    assert p.ProcedureGenerator.convert_python_type_to_sql(bool) == "boolean"
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.datetime) == "timestamp without time zone"


def test_convert_python_type_to_sql_array():
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.Array[bool]) == "boolean[]"


def test_convert_python_type_to_sql_2d_array():
    assert p.ProcedureGenerator.convert_python_type_to_sql(p.Array[p.Array[bool]]) == "boolean[][]"


def test_generate_return_type_void():
    assert p.ProcedureGenerator.generate_return_type(example_1) == "void"


def test_generate_return_type_simple():
    assert p.ProcedureGenerator.generate_return_type(example_3) == "boolean"


def test_generate_return_type_sequence():
    assert p.ProcedureGenerator.generate_return_type(example_5) == "SETOF int"


def test_convert_value_to_sql_none():
    assert p.ProcedureGenerator.convert_python_value_to_sql(None) == "'NULL'"


def test_convert_value_to_sql_boolean():
    assert p.ProcedureGenerator.convert_python_value_to_sql(True) == "'True'"


def test_convert_value_to_sql_boolean():
    assert p.ProcedureGenerator.convert_python_value_to_sql(1.0) == "'1.0'"


def test_convert_value_to_sql_datetime():
    now = p.datetime.utcnow()
    date_string = p.ProcedureGenerator.convert_python_value_to_sql(now)
    assert date_string == "'%s'" % now.isoformat()


def test_convert_value_to_sql_array():
    my_array = [1, 2, 3]
    array_string = p.ProcedureGenerator.convert_python_value_to_sql(my_array)
    assert array_string == "'{1, 2, 3}'"


def test_convert_value_to_sql_array_2d():
    my_array = [[1, 2], [3, 4]]
    array_string = p.ProcedureGenerator.convert_python_value_to_sql(my_array)
    assert array_string == "'{{1, 2}, {3, 4}}'"


def test_generate_sql_default_value_nodefault():
    type_and_default = p.ProcedureGenerator.generate_sql_default_value("boolean", True)
    assert type_and_default == "boolean DEFAULT 'True'"


if __name__ == "__main__":
    pass
    # pytest.main()
