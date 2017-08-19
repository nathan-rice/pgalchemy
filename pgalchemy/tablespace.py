
class Tablespace(object):

    _sql_create_template = """
    """

    def __init__(self, name, location=''):
        self.name = name
        self.location = location