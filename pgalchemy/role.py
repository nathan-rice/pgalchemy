from pgalchemy.types import Creatable
from pgalchemy.util import get_name


class Role(Creatable):

    _sql_create_template = """
        CREATE ROLE {name} {with_options}
    """

    _sql_drop_template = """
        DROP ROLE IF EXISTS {name}
    """

    def __init__(self, name, options=None):
        self.name = name
        self._options = options or {}

    def _set_boolean_option(self, option_name, value):
        option_string = "NO " + option_name if not value else option_name
        self._options[option_name] = option_string
        self._last_option = option_name

    def __call__(self, value) -> 'Role':
        self._set_boolean_option(self._last_option, value)
        return self

    @property
    def _create_statement(self):
        options = " ".join(self._options.values())
        if options:
            options = "WITH " + options
        return self._sql_create_template.format(name=self.name, with_options=options)

    @property
    def _drop_statement(self):
        return self._sql_drop_template.format(name=self.name)

    @property
    def superuser(self) -> 'Role':
        self._set_boolean_option("SUPERUSER", True)
        return self

    @property
    def create_db(self) -> 'Role':
        self._set_boolean_option("CREATEDB", True)
        return self

    @property
    def create_role(self) -> 'Role':
        self._set_boolean_option("CREATEROLE", True)
        return self

    @property
    def inherits_privileges(self) -> 'Role':
        self._set_boolean_option("INHERIT", True)
        return self

    @property
    def login(self) -> 'Role':
        self._set_boolean_option("LOGIN", True)
        return self

    @property
    def replication(self) -> 'Role':
        self._set_boolean_option("REPLICATION", True)
        return self

    def connection_limit(self, connection_limit=-1) -> 'Role':
        self._options["CONNECTION_LIMIT"] = "CONNECTION LIMIT %s" % connection_limit
        return self

    def password(self, password, encrypted=True, valid_until=None) -> 'Role':
        password_option = "ENCRYPTED PASSWORD '%s'" % password
        if not encrypted:
            password_option = "UN" + password
        if valid_until:
            password_option += " VALID UNTIL '%s'" % valid_until.isoformat()
        self._options["PASSWORD"] = password_option
        return self

    def in_role(self, *roles) -> 'Role':
        if roles:
            role_names = ", ".join(get_name(r) for r in roles)
            self._options["IN ROLE"] = "IN ROLE %s" % role_names
        return self

    def including_roles(self, *roles) -> 'Role':
        if roles:
            role_names = ", ".join(get_name(r) for r in roles)
            self._options["ROLE"] = "ROLE %s" % role_names
        return self

    def including_admins(self, *roles) -> 'Role':
        if roles:
            role_names = ", ".join(get_name(r) for r in roles)
            self._options["ADMIN"] = "ADMIN %s" % role_names
        return self

