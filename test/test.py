import typing
import inspect
import ast
import pytest
import postgresalchemy

def example_function(a: int = 1, b: bool = True,
                     c: typing.Sequence[int] = (1, 2, 3)) -> bool: # test
    print("foo")
    return True

def test_example():
    f = example_function
    signature = inspect.signature(f)
    code = inspect.getsourcelines(f)
    print(code)
    parsed = ast.parse(inspect.getsource(f))
    print(parsed)
    #f = postgresalchemy.wrap(example_function)

if __name__ == "__main__":
    test_example()
    #pytest.main("test.py")