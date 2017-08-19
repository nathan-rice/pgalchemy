import pytest
import pgalchemy.privilege as p
from .config import *


def test_command_option_str():
    option_1 = p.CommandOption("INSERT", test_table.c.id)
    assert str(option_1) == "INSERT (test_table.id)"
    option_2 = p.CommandOption("SELECT", "id")
    assert str(option_2) == "SELECT (id)"


def test_table_privilege():
    privilege_1 = p.Privilege()
    privilege_1.select.insert(test_table.c.id).update.delete.truncate.references.trigger
    privilege_1.on.table(test_table).to("nathan")
    assert [c.name for c in privilege_1._commands] == ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES",
                                                     "TRIGGER"]
    assert privilege_1._target == [test_table]
    assert privilege_1._target_type == "TABLE"
    privilege_1.with_grant_option
    assert privilege_1._with_grant_option
    privilege_2 = p.Privilege()
    privilege_2.select.truncate.on.all.tables_in_schema("PUBLIC")
    assert [c.name for c in privilege_2._commands] == ["SELECT", "TRUNCATE"]
    assert privilege_2._target == ["ALL TABLES IN SCHEMA PUBLIC"]
    assert not privilege_2._with_grant_option
    privilege_3 = p.Privilege()
    privilege_3.all.on.table(test_table).to("nathan")
    assert [c.name for c in privilege_3._commands] == ["ALL"]


def test_sequence_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.select.update.on.sequence("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ['USAGE', 'SELECT', 'UPDATE']
    assert privilege_1._target_type == "SEQUENCE"
    assert privilege_1._target == ["foo"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.sequence("foo").to("nathan", "PUBLIC")
    assert [c.name for c in privilege_2._commands] == ["ALL"]
    assert privilege_2._target_type == "SEQUENCE"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]
    privilege_3 = p.Privilege()
    privilege_3.usage.on.all.sequences_in_schema("foo", "PUBLIC")
    assert [c.name for c in privilege_3._commands] == ["USAGE"]
    assert privilege_3._target == ["ALL SEQUENCES IN SCHEMA foo, PUBLIC"]


def test_database_privilege():
    privilege_1 = p.Privilege()
    privilege_1.create.connect.temporary.on.database("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ['CREATE', 'CONNECT', 'TEMPORARY']
    assert privilege_1._target_type == "DATABASE"
    assert privilege_1._target == ["foo"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.database("foo").to("nathan", "PUBLIC")
    assert [c.name for c in privilege_2._commands] == ["ALL"]
    assert privilege_2._target_type == "DATABASE"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_domain_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.on.domain("foo", "bar").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["USAGE"]
    assert privilege_1._target_type == "DOMAIN"
    assert privilege_1._target == ["foo", "bar"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.domain("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "DOMAIN"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_foreign_data_wrapper_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.on.foreign_data_wrapper("foo", "bar").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["USAGE"]
    assert privilege_1._target_type == "FOREIGN DATA WRAPPER"
    assert privilege_1._target == ["foo", "bar"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.foreign_data_wrapper("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "FOREIGN DATA WRAPPER"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_foreign_server_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.on.foreign_server("foo", "bar").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["USAGE"]
    assert privilege_1._target_type == "FOREIGN SERVER"
    assert privilege_1._target == ["foo", "bar"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.foreign_server("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "FOREIGN SERVER"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_function_privilege():
    privilege_1 = p.Privilege()
    privilege_1.execute.on.function("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["EXECUTE"]
    assert privilege_1._target_type == "FUNCTION"
    assert privilege_1._target == ["foo"]
    assert privilege_1._recipient == ["nathan"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.function("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "FUNCTION"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]
    privilege_3 = p.Privilege()
    privilege_3.execute.on.all.functions_in_schema("foo", "PUBLIC")
    assert [c.name for c in privilege_3._commands] == ["EXECUTE"]
    assert privilege_3._target == ["ALL FUNCTIONS IN SCHEMA foo, PUBLIC"]


def test_language_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.on.language("foo", "bar").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["USAGE"]
    assert privilege_1._target_type == "LANGUAGE"
    assert privilege_1._target == ["foo", "bar"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.language("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "LANGUAGE"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_large_object_privilege():
    privilege_1 = p.Privilege()
    privilege_1.select.update.on.large_object("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["SELECT", "UPDATE"]
    assert privilege_1._target_type == "LARGE OBJECT"
    assert privilege_1._target == ["foo"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.large_object("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "LARGE OBJECT"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_schema_privilege():
    privilege_1 = p.Privilege()
    privilege_1.create.usage.on.schema("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ['CREATE', 'USAGE']
    assert privilege_1._target_type == "SCHEMA"
    assert privilege_1._target == ["foo"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.schema("foo").to('nathan', 'PUBLIC')
    assert privilege_2._target_type == "SCHEMA"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_tablespace_privilege():
    privilege_1 = p.Privilege()
    privilege_1.create.on.tablespace("foo").to("nathan")
    assert [c.name for c in privilege_1._commands] == ['CREATE']
    assert privilege_1._target_type == "TABLESPACE"
    assert privilege_1._target == ["foo"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.tablespace("foo").to('nathan', 'PUBLIC')
    assert privilege_2._target_type == "TABLESPACE"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]


def test_type_privilege():
    privilege_1 = p.Privilege()
    privilege_1.usage.on.type("foo", "bar").to("nathan")
    assert [c.name for c in privilege_1._commands] == ["USAGE"]
    assert privilege_1._target_type == "TYPE"
    assert privilege_1._target == ["foo", "bar"]
    privilege_2 = p.Privilege()
    privilege_2.all.on.type("foo").to("nathan", "PUBLIC")
    assert privilege_2._target_type == "TYPE"
    assert privilege_2._target == ["foo"]
    assert privilege_2._recipient == ["nathan", "PUBLIC"]

