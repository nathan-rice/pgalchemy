from sqlalchemy.types import TypeEngine
from sqlalchemy.sql.visitors import VisitableType
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnClause
from sqlalchemy.sql.compiler import GenericTypeCompiler
from .util import get_condition_text, camelcase_to_underscore, before_create, after_create, execute_if_postgres


class _Value(ColumnClause):
    pass


@compiles(_Value)
def _value_compiler(element, compiler, **kwargs):
    return "VALUE"


VALUE = _Value("VALUE")


class DomainMeta(VisitableType):
    def __new__(mcs, name, bases, class_dict):
        sqlalchemy_types = [base for base in bases if issubclass(base, TypeEngine)]
        if "type" not in class_dict:
            for type_ in sqlalchemy_types:
                class_dict["type"] = type_
                break
        elif not sqlalchemy_types:
            sqlalchemy_type = class_dict["type"]
            if isinstance(class_dict["type"], type):
                bases += (sqlalchemy_type,)
            else:
                # Handle the case where the user specifies an instance instead of a type in order to provide parameters
                bases += (type(sqlalchemy_type))
        if "name" not in class_dict:
            class_dict["name"] = camelcase_to_underscore(name)
        class_dict["__visit_name__"] = class_dict["name"]
        domain = super().__new__(mcs, name, bases, class_dict)
        if "type" in class_dict:
            def visit_domain(cls, type_, **kw):
                return type_.name

            setattr(GenericTypeCompiler, "visit_" + domain.name, visit_domain)
            before_create(domain._grant)
            after_create(domain._revoke)
        return domain


class Domain(metaclass=DomainMeta):
    _create_sql_template = """
        CREATE DOMAIN {name} AS {type} {collate} {default} {constraint}
    """

    _drop_sql_template = """
        DROP DOMAIN {name}
    """

    @classmethod
    def _create(cls, target, connection, **kwargs):
        type_ = cls.type.compile(connection.dialect)
        collate = "COLLATE %s" % cls.collate if hasattr(cls, "collate") else ""
        default = "DEFAULT %s" % cls.default if hasattr(cls, "default") else ""
        constraint = "CHECK (%s)" % get_condition_text(cls.constraint) if hasattr(cls, "constraint") else ""
        statement = cls._create_sql_template.format(name=cls.name, type=type_, collate=collate, default=default,
                                                    constraint=constraint)
        execute_if_postgres(connection, statement)

    @classmethod
    def _drop(cls, target, connection, **kwargs):
        statement = cls._drop_sql_template.format(name=cls.name)
        execute_if_postgres(connection, statement)
