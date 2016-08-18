from types import FunctionType
from .types import ValueSetter, FluentClauseContainer, PostgresOption
from .util import get_name, get_table_name, get_role_name, before_create, after_create, before_drop, is_postgres, \
    execute_if_postgres, get_name
from .function import FunctionGenerator


class PrivilegeClause(object):
    def __init__(self, privilege):
        self._privilege = privilege
        self._privilege._current_clause = self

    @property
    def _grant_statement(self):
        return self._privilege._grant_statement

    @property
    def _revoke_statement(self):
        return self._privilege._revoke_statement


class CommandOption(PostgresOption):
    def __init__(self, command_name, *columns):
        self.name = command_name
        self.columns = columns

    def __str__(self):
        if self.columns:
            column_names = ", ".join(get_name(c) for c in self.columns)
            as_str = "%s (%s)" % (self.name, column_names)
        else:
            as_str = self.name
        return as_str

    def __eq__(self, other):
        return self.name == other.name and self.columns == other.columns

    def __hash__(self):
        return hash(str(self))


class WithGrantOption(PrivilegeClause):
    @property
    def with_grant_option(self) -> PrivilegeClause:
        self._privilege._with_grant_option = True
        return PrivilegeClause(self._privilege)


class GrantTo(PrivilegeClause):
    def to(self, *roles) -> WithGrantOption:
        self._privilege._set_recipients(roles)
        return WithGrantOption(self._privilege)


class ExecuteOnInSchema(PrivilegeClause):
    def functions_in_schema(self, *schemas) -> GrantTo:
        schema_names = ", ".join(schemas)
        self._privilege._set_targets("ALL FUNCTIONS IN SCHEMA %s" % schema_names)
        return GrantTo(self._privilege)


class SequenceOnInSchema(PrivilegeClause):
    def sequences_in_schema(self, *schemas) -> GrantTo:
        schema_names = ", ".join(schemas)
        self._privilege._set_targets("ALL SEQUENCES IN SCHEMA %s" % schema_names)
        return GrantTo(self._privilege)


class TableOnInSchema(PrivilegeClause):
    def tables_in_schema(self, *schemas) -> GrantTo:
        schema_names = ", ".join(schemas)
        self._privilege._set_targets("ALL TABLES IN SCHEMA %s" % schema_names)
        return GrantTo(self._privilege)


class SelectOrUpdateOnInSchema(SequenceOnInSchema, TableOnInSchema):
    """Placeholder docstring"""


class AllOnInSchema(SelectOrUpdateOnInSchema, ExecuteOnInSchema):
    """Placeholder docstring"""


class OnBase(PrivilegeClause):
    def _set_on(self, target_type, targets):
        self._privilege._target_type = target_type
        self._privilege._set_targets(targets)  # Converting here to avoid tuple-in-a-list
        return GrantTo(self._privilege)


class TableOn(OnBase):
    def table(self, *tables) -> GrantTo:
        return self._set_on("TABLE", tables)


class ExpandedTableOn(TableOn):
    @property
    def all(self) -> TableOnInSchema:
        return TableOnInSchema(self._privilege)


class SequenceOn(OnBase):
    def sequence(self, *sequences) -> GrantTo:
        return self._set_on("SEQUENCE", sequences)

    @property
    def all(self) -> SequenceOnInSchema:
        return SequenceOnInSchema(self._privilege)


class DatabaseOn(OnBase):
    def database(self, *databases) -> GrantTo:
        return self._set_on("DATABASE", databases)


class DomainOn(OnBase):
    def domain(self, *domains) -> GrantTo:
        return self._set_on("DOMAIN", domains)


class ForeignDataWrapperOn(OnBase):
    def foreign_data_wrapper(self, *foreign_data_wrappers) -> GrantTo:
        return self._set_on("FOREIGN DATA WRAPPER", foreign_data_wrappers)


class ForeignServerOn(OnBase):
    def foreign_server(self, *foreign_servers) -> GrantTo:
        return self._set_on("FOREIGN SERVER", foreign_servers)


class LanguageOn(OnBase):
    def language(self, *languages) -> GrantTo:
        return self._set_on("LANGUAGE", languages)


class LargeObjectOn(OnBase):
    def large_object(self, *large_objects) -> GrantTo:
        return self._set_on("LARGE OBJECT", large_objects)


class SchemaOn(OnBase):
    def schema(self, *schemas) -> GrantTo:
        return self._set_on("SCHEMA", schemas)


class TablespaceOn(OnBase):
    def tablespace(self, *table_spaces) -> GrantTo:
        return self._set_on("TABLESPACE", table_spaces)


class TypeOn(OnBase):
    def type(self, *types) -> GrantTo:
        return self._set_on("TYPE", types)


class ExecuteOn(OnBase):
    def function(self, *functions) -> GrantTo:
        return self._set_on("FUNCTION", functions)

    @property
    def all(self) -> ExecuteOnInSchema:
        return ExecuteOnInSchema(self._privilege)


class CreateOn(DatabaseOn, SchemaOn, TablespaceOn):
    """Placeholder docstring"""


class SelectOrUpdateOn(TableOn, SequenceOn, LargeObjectOn):
    @property
    def all(self) -> SelectOrUpdateOnInSchema:
        return SelectOrUpdateOnInSchema(self._privilege)


class UsageOn(SequenceOn, DomainOn, ForeignDataWrapperOn, ForeignServerOn, LanguageOn, TypeOn):
    @property
    def all(self) -> SequenceOnInSchema:
        return SequenceOnInSchema(self._privilege)


class AllOn(UsageOn, SelectOrUpdateOn, ExecuteOn, CreateOn):
    @property
    def all(self) -> AllOnInSchema:
        return AllOnInSchema(self._privilege)


class SchemaOnConnector(PrivilegeClause):
    @property
    def on(self) -> SchemaOn:
        return SchemaOn(self._privilege)


class ExecuteOnConnector(PrivilegeClause):
    @property
    def on(self) -> ExecuteOn:
        return ExecuteOn(self._privilege)


class AllOnConnector(PrivilegeClause):
    @property
    def on(self) -> AllOn:
        return AllOn(self._privilege)


class UsageCommandBase(PrivilegeClause):
    @property
    def usage(self) -> 'UsageCommand':
        self._privilege._set_commands(CommandOption("USAGE"))
        return UsageCommand(self._privilege)


class SelectCommandBase(PrivilegeClause):
    @property
    def select(self) -> 'SelectCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return SelectCommand(self._privilege)


class UpdateCommandBase(PrivilegeClause):
    @property
    def update(self) -> 'UpdateCommand':
        self._privilege._set_commands(CommandOption("UPDATE"))
        return UpdateCommand(self._privilege)


class CreateCommandBase(PrivilegeClause):
    @property
    def create(self) -> 'CreateCommand':
        self._privilege._set_commands(CommandOption("CREATE"))
        return CreateCommand(self._privilege)


class TableCommandBase(PrivilegeClause):
    @property
    def select(self) -> 'CallableTableCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return CallableTableCommand(self._privilege)

    @property
    def insert(self) -> 'CallableTableCommand':
        self._privilege._set_commands(CommandOption("INSERT"))
        return CallableTableCommand(self._privilege)

    @property
    def update(self) -> 'CallableTableCommand':
        self._privilege._set_commands(CommandOption("UPDATE"))
        return CallableTableCommand(self._privilege)

    @property
    def references(self) -> 'CallableTableCommand':
        self._privilege._set_commands(CommandOption("REFERENCES"))
        return CallableTableCommand(self._privilege)

    @property
    def delete(self) -> 'TableCommand':
        self._privilege._set_commands(CommandOption("DELETE"))
        return TableCommand(self._privilege)

    @property
    def truncate(self) -> 'TableCommand':
        self._privilege._set_commands(CommandOption("TRUNCATE"))
        return TableCommand(self._privilege)

    @property
    def trigger(self) -> 'TableCommand':
        self._privilege._set_commands(CommandOption("TRIGGER"))
        return TableCommand(self._privilege)

    @property
    def all(self) -> 'TableCommand':
        self._privilege._set_commands(CommandOption("ALL"))
        return TableCommand(self._privilege)


class TableCommand(TableCommandBase):
    @property
    def on(self) -> ExpandedTableOn:
        return ExpandedTableOn(self._privilege)


class CallableTableCommand(TableCommand):
    def __call__(self, *columns):
        self._privilege._commands[-1].columns = columns
        return TableColumnCommand(self._privilege)


class TableColumnCommand(CallableTableCommand):
    # Properties are duplicated to support optimal code completion
    @property
    def select(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return TableColumnCommand(self._privilege)

    @property
    def insert(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("INSERT"))
        return TableColumnCommand(self._privilege)

    @property
    def update(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("UPDATE"))
        return TableColumnCommand(self._privilege)

    @property
    def references(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("REFERENCES"))
        return TableColumnCommand(self._privilege)

    @property
    def delete(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("DELETE"))
        return TableColumnCommand(self._privilege)

    @property
    def truncate(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("TRUNCATE"))
        return TableColumnCommand(self._privilege)

    @property
    def trigger(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("TRIGGER"))
        return TableColumnCommand(self._privilege)

    @property
    def all(self) -> 'TableColumnCommand':
        self._privilege._set_commands(CommandOption("ALL"))
        return TableColumnCommand(self._privilege)

    @property
    def on(self) -> TableOn:
        return TableOn(self._privilege)


class SequenceCommand(PrivilegeClause):
    @property
    def on(self) -> SequenceOn:
        return SequenceOn(self._privilege)

    @property
    def select(self) -> 'SequenceCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return self

    @property
    def update(self) -> 'SequenceCommand':
        self._privilege._set_commands(CommandOption("UPDATE"))
        return self


class DatabaseCommandBase(PrivilegeClause):
    @property
    def create(self) -> 'DatabaseCommand':
        self._privilege._set_commands(CommandOption("CREATE"))
        return self

    @property
    def connect(self) -> 'DatabaseCommand':
        self._privilege._set_commands(CommandOption("CONNECT"))
        return self

    @property
    def temporary(self) -> 'DatabaseCommand':
        self._privilege._set_commands(CommandOption("TEMPORARY"))
        return self


class DatabaseCommand(DatabaseCommandBase):
    @property
    def on(self) -> DatabaseOn:
        return DatabaseOn(self._privilege)


class FunctionCommand(PrivilegeClause):
    @property
    def execute(self) -> 'ExecuteOnConnector':
        self._privilege._set_commands(CommandOption("EXECUTE"))
        return ExecuteOnConnector(self._privilege)


class UsageCommand(PrivilegeClause):
    @property
    def select(self) -> 'SequenceCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return SequenceCommand(self._privilege)

    @property
    def update(self) -> 'SequenceCommand':
        self._privilege._set_commands(CommandOption("SELECT"))
        return SequenceCommand(self._privilege)

    @property
    def on(self) -> UsageOn:
        return UsageOn(self._privilege)


class SelectOrUpdateCommand(CallableTableCommand):
    @property
    def usage(self) -> SequenceCommand:
        self._privilege._set_commands(CommandOption("USAGE"))
        return SequenceCommand(self._privilege)

    @property
    def on(self) -> SelectOrUpdateOn:
        return SelectOrUpdateOn(self._privilege)


class SelectCommand(SelectOrUpdateCommand):
    @property
    def update(self) -> 'UpdateCommand':
        self._privilege._set_commands(CommandOption("UPDATE"))
        return UpdateCommand(self._privilege)


class UpdateCommand(SelectOrUpdateCommand):
    @property
    def select(self) -> SelectCommand:
        self._privilege._set_commands(CommandOption("UPDATE"))
        return SelectCommand(self._privilege)


class CreateCommand(PrivilegeClause):
    @property
    def connect(self) -> DatabaseCommand:
        self._privilege._set_commands(CommandOption("CONNECT"))
        return DatabaseCommand(self._privilege)

    @property
    def temporary(self) -> DatabaseCommand:
        self._privilege._set_commands(CommandOption("TEMPORARY"))
        return DatabaseCommand(self._privilege)

    @property
    def usage(self) -> SchemaOnConnector:
        self._privilege._set_commands(CommandOption("USAGE"))
        return SchemaOnConnector(self._privilege)

    @property
    def on(self) -> CreateOn:
        return CreateOn(self._privilege)


class Privilege(UsageCommandBase, SelectCommandBase, UpdateCommandBase, CreateCommandBase, FunctionCommand,
                TableCommandBase, DatabaseCommandBase, FluentClauseContainer):
    _sql_grant_template = """
        GRANT {commands} on {target_type} {targets} to {recipients}
    """

    _sql_revoke_template = """
        REVOKE {commands} on {target_type} {targets} from {recipients}
    """

    def __init__(self, commands=None, target_type=None, targets=None, recipients=None, with_grant_option=False):
        self._commands = []
        self._target = []
        self._recipient = []
        self._set_commands(commands)
        self._target_type = target_type
        self._set_targets(targets)
        self._set_recipients(recipients)
        self._with_grant_option = with_grant_option
        self._privilege = self  # required to support the various command mix-ins

    def _set_commands(self, commands):
        ValueSetter.set(self._commands, commands)

    def _set_targets(self, target):
        ValueSetter.set(self._target, target)

    def _set_recipients(self, recipient):
        ValueSetter.set(self._recipient, recipient)

    def _format_target(self):
        def format_function(f):
            if isinstance(f, str):
                return f
            elif isinstance(f, FunctionType):
                parameters = FunctionGenerator.get_parameters(f)
                sql_types = [FunctionGenerator.convert_python_type_to_sql(p.annotation) for p in parameters]
                return "%s(%s)" % (f.__name__, ", ".join(sql_types))

        if self._target_type == "FUNCTION":
            target = [format_function(f) for f in self._target]
        elif self._target_type == "TABLE":
            target = [get_table_name(t) for t in self._target]
        else:
            target = self._target
        return ", ".join(target)

    def _statement(self, template):
        commands = ", ".join(str(command) for command in self._commands)
        target_type = self._target_type or ""
        target_names = [get_name(s) for s in self._target]
        targets = ", ".join(target_names)
        recipient_names = [get_name(r) for r in self._recipient]
        recipients = ", ".join(recipient_names)
        return template.format(commands=commands, target_type=target_type, targets=targets, recipients=recipients)

    @property
    def _grant_statement(self):
        return self._statement(self._sql_grant_template)

    @property
    def _revoke_statement(self):
        return self._statement(self._sql_revoke_template)

    @property
    def all(self) -> AllOnConnector:
        self._privilege._set_commands(CommandOption("ALL"))
        return AllOnConnector(self._privilege)


def grant(*privileges, connection=None):
    for privilege in privileges:
        statement = privilege._grant_statement
        if connection:
            execute_if_postgres(connection, statement)
        else:
            after_create(statement)


def revoke(*privileges, connection=None):
    for privilege in privileges:
        statement = privilege._revoke_statement
        if connection:
            execute_if_postgres(connection, statement)
        else:
            after_create(statement)
