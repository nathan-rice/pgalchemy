from sqlalchemy.types import TypeEngine
from sqlalchemy.sql.visitors import VisitableType
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnClause
from sqlalchemy.sql.compiler import GenericTypeCompiler
import re

_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')


def _camelcase_to_underscore(name):
    s1 = _first_cap_re.sub(r'\1_\2', name)
    return _all_cap_re.sub(r'\1_\2', s1).lower()


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
            bases += (class_dict["type"],)
        if "name" not in class_dict:
            class_dict["name"] = _camelcase_to_underscore(name)
        class_dict["__visit_name__"] = class_dict["name"]
        domain = super().__new__(mcs, name, bases, class_dict)
        if "type" in class_dict:
            def visit_domain(cls, type_, **kw):
                return type_.name

            setattr(GenericTypeCompiler, "visit_" + domain.name, visit_domain)
        return domain


class Domain(metaclass=DomainMeta):
    pass
