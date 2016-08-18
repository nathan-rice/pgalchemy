from typing import Sequence

from .util import get_condition_text, get_name
from .types import FluentClauseContainer, ValueSetter, DependentCreatable


class PolicyClause(object):
    def __init__(self, policy):
        self._policy = policy
        self._policy._current_clause = self


class PolicyCheckClause(PolicyClause):
    def with_check(self, expression) -> PolicyClause:
        self._policy._check = expression
        return PolicyClause(self._policy)


class PolicyUsingClause(PolicyClause):
    def using(self, expression) -> PolicyCheckClause:
        self._policy._using = expression
        return PolicyCheckClause(self._policy)


class PolicyToClause(PolicyClause):
    def to(self, *roles) -> PolicyUsingClause:
        self._policy._set_recipient(roles)
        return PolicyUsingClause(self._policy)


class PolicyForClause(PolicyClause):
    @property
    def all(self) -> PolicyToClause:
        self._policy._command = "ALL"
        return PolicyToClause(self._policy)

    @property
    def select(self) -> PolicyToClause:
        self._policy._command = "SELECT"
        return PolicyToClause(self._policy)

    @property
    def insert(self) -> PolicyToClause:
        self._policy._command = "INSERT"
        return PolicyToClause(self._policy)

    @property
    def update(self) -> PolicyToClause:
        self._policy._command = "UPDATE"
        return PolicyToClause(self._policy)

    @property
    def delete(self) -> PolicyToClause:
        self._policy._command = "DELETE"
        return PolicyToClause(self._policy)


class PolicyOnClause(PolicyToClause, PolicyUsingClause, PolicyCheckClause):
    @property
    def for_(self) -> PolicyForClause:
        return PolicyForClause(self._policy)


class Policy(FluentClauseContainer, DependentCreatable):
    _sql_create_template = """
        CREATE POLICY {name} on {table_name} {for_command} {to_recipient} {using_expression} {with_check_expression}
    """

    _sql_drop_template = """
        DROP POLICY IF EXISTS {name} on {table_name}
    """

    def __init__(self, name, table=None, command=None, recipient=None, using=None, check=None):
        self._name = name
        self._table = table
        self._command = command
        self._recipient = []
        self._set_recipient(recipient)
        self._using = using
        self._check = check
        self._policy = self
        self._current_clause = None

    @property
    def _create_statement(self):
        table_name = get_name(self._table)
        for_command = "FOR %s" % self._command if self._command else ''
        to_recipient = "TO %s" % ', '.join(self._recipient) if self._recipient else ''
        using_expression = "USING (%s)" % get_condition_text(self._using) if self._using else ''
        with_check_expression = "WITH CHECK (%s)" % get_condition_text(self._check) if self._check else ''
        return self._sql_create_template.format(name=self._name, table_name=table_name, for_command=for_command,
                                                to_recipient=to_recipient, using_expression=using_expression,
                                                with_check_expression=with_check_expression)

    @property
    def _drop_statement(self):
        table_name = get_name(self._table)
        return self._sql_drop_template.format(name=self._name, table_name=table_name)

    def _set_recipient(self, recipient):
        ValueSetter.set(self._recipient, recipient)

    def on(self, table) -> PolicyOnClause:
        self._table = table
        return PolicyOnClause(self)
