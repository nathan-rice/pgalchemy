import pytest
from pgalchemy import trigger as t
from .config import *


def test_trigger_wrong_return_annotation():
    trigger = t.Trigger("test")
    with pytest.raises(ValueError):
        trigger.before.insert.update.on(test_table).for_each.row(example_6)


def test_trigger_noargs():
    trigger = t.Trigger("test")
    trigger.before.insert.update.on(test_table).from_table(test_table).deferrable.deferred.for_each.row(example_7)
    assert trigger._defer == "DEFERRABLE INITIALLY DEFERRED"
    assert trigger._execution_time == "BEFORE"
    assert trigger._event == ["INSERT", "UPDATE"]
    assert trigger._selectable == test_table
    assert trigger._cardinality == "FOR EACH ROW"
    assert trigger._function == "example_7"


def test_trigger_args():
    trigger = t.Trigger("test")
    trigger.before.insert.update.on(test_table).deferrable.immediate.for_each.row.with_arguments("a", "b", "c")
    trigger(example_8)
    assert trigger._defer == "DEFERRABLE INITIALLY IMMEDIATE"
    assert trigger._arguments == ("a", "b", "c")


def test_trigger_wrong_number_of_args():
    trigger = t.Trigger("test")
    trigger.before.insert.update.on(test_table).for_each.row.with_arguments("a")
    with pytest.raises(ValueError):
        trigger(example_8)


def test_trigger_with_default_arguments():
    trigger = t.Trigger("test")
    trigger.before.insert.update.on(test_table).for_each.row.with_arguments("a")(example_9)
    assert trigger._arguments == ("a",)


def test_trigger_with_default_arguments_2():
    trigger = t.Trigger(example_9).before.insert.update.on(test_table).for_each.row.with_arguments("a", "b")
    assert trigger._arguments == ("a", "b")


def test_trigger_with_sqlalchemy_table():
    trigger = t.Trigger("test")
    trigger.after \
        .insert \
        .update_of(test_table.c.name, test_table.c.id) \
        .on(test_table) \
        .for_each(example_7)
    assert trigger._event == ["INSERT", "UPDATE OF test_table.name, test_table.id"]
    assert trigger._selectable == test_table


def test_trigger_with_sqlalchemy_class():
    trigger = t.Trigger("test")
    trigger.after \
        .insert \
        .update_of(TestMappedClass.name, TestMappedClass.id) \
        .on(TestMappedClass)
    trigger.for_each(example_7)
    assert trigger._event == ["INSERT", "UPDATE OF test_table.name, test_table.id"]
    assert trigger._selectable == test_table


def test_constraint_trigger():
    trigger = t.ConstraintTrigger("test")
    trigger.insert.update.on(test_table).for_each.row(example_7)
    assert trigger._constraint == "CONSTRAINT"
    assert trigger._execution_time == "AFTER"


def test_trigger_timing_exception_1():
    trigger = t.Trigger("test")
    with pytest.raises(ValueError):
        trigger.instead_of.insert.on(test_table).for_each.statement


def test_trigger_timing_exception_2():
    trigger = t.Trigger("test")
    with pytest.raises(ValueError):
        trigger.instead_of.truncate.on(test_table).for_each.statement
