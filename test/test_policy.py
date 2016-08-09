import pytest
import postgresalchemy.policy as p
from .config import *


def test_policy_all():
    policy = p.Policy("test")
    policy.on(test_table).for_.all.to("nathan").using(test_table.c.id == 1).with_check(test_table.c.name == "nathan")
    assert policy._table == test_table
    assert policy._command == "ALL"
    assert policy._recipient == ["nathan"]
    assert str(policy._using) == str(test_table.c.id == 1)
    assert str(policy._check) == str(test_table.c.name == "nathan")


def test_policy_select():
    policy = p.Policy("test")
    policy.on(test_table).for_.select.to("nathan", "CURRENT_USER")
    assert policy._table == test_table
    assert policy._command == "SELECT"
    assert policy._recipient == ["nathan", "CURRENT_USER"]


def test_policy_insert():
    policy = p.Policy("test")
    policy.on(test_table).for_.insert.to("nathan", "CURRENT_USER")
    assert policy._table == test_table
    assert policy._command == "INSERT"
    assert policy._recipient == ["nathan", "CURRENT_USER"]


def test_policy_update():
    policy = p.Policy("test")
    policy.on(test_table).for_.update.to("nathan", "CURRENT_USER")
    assert policy._table == test_table
    assert policy._command == "UPDATE"
    assert policy._recipient == ["nathan", "CURRENT_USER"]


def test_policy_delete():
    policy = p.Policy("test")
    policy.on(test_table).for_.delete.to("nathan", "CURRENT_USER")
    assert policy._table == test_table
    assert policy._command == "DELETE"
    assert policy._recipient == ["nathan", "CURRENT_USER"]