from typing import Sequence
from sqlalchemy import MetaData, Table, Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from postgresalchemy.function import Array
from postgresalchemy.trigger import Trigger

metadata = MetaData()
Base = declarative_base(metadata=metadata)
test_table = Table("test_table", metadata, Column("id", Integer, primary_key=True), Column("name", Text))


class TestMappedClass(Base):
    __table__ = test_table


mapped_instance = TestMappedClass(id=1, name="test")


def example_1():
    return True


def example_2(a: int = 1):
    return True


def example_3(a: int = 1) -> bool:
    return True


def example_4(a: int = 1, b: bool = True,
              c: Array[int] = (1, 2, 3)) -> bool:  # test
    return True


def example_5(a: int = 1, b: bool = True,
              c: Array[int] = (1, 2, 3)) -> Sequence[int]:  # test
    a = 2
    b = False
    return c


def example_6(a: TestMappedClass) -> Sequence[TestMappedClass]:  # test
    return 1, 2, 3


def example_7() -> Trigger:
    return True


def example_8(a, b, c) -> Trigger:
    return True


def example_9(a, b=1, c=2) -> Trigger:
    return True
