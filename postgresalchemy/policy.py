
class PolicyClause(object):
    def __init__(self, policy):
        self._policy = policy


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
        self._policy._recipient = roles
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


class Policy(PolicyToClause, PolicyUsingClause, PolicyCheckClause):
    def __init__(self, name, table=None, command=None, recipient=None, using=None, check=None):
        self._name = name
        self._table = table
        self._command = command
        self._recipient = recipient
        self._using = using
        self._check = check
        self._policy = self

    @property
    def for_(self) -> PolicyForClause:
        return PolicyForClause(self)
