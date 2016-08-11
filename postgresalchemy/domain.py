from sqlalchemy.types import TypeDecorator


class Domain(TypeDecorator):
    def __init__(self, name):
        pass

    def get_dbapi_type(self, dbapi):
        return self.type_name