import pytest
import postgresalchemy.privilege as p
from .config import *


def test_table_column_privilege():
    privilege = p.Privilege()
    privilege.select.insert(test_table.c.id).update.delete.truncate.references.trigger
    privilege.on.table(test_table).to("nathan").with_grant_option
    assert [c.name for c in privilege._commands] == ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES",
                                                     "TRIGGER"]
    assert privilege._target == [test_table]
    assert privilege._target_type == "TABLE"
    assert privilege._with_grant_option == True


if __name__ == "__main__":
    test_table_column_privilege()