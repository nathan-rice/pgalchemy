from collections import OrderedDict


class PostgresAlchemy(object):

    def __init__(self, connection=None):
        self.engine = connection
        self.roles = OrderedDict()
        self.procedure = OrderedDict()
        self.triggers = OrderedDict()
        self.privileges = OrderedDict()
        self.policies = OrderedDict()

    def role(self):
        pass

    def procedure(self):
        pass

    def trigger(self):
        pass

    def policy(self):
        pass

    def grant(self):
        pass

    def revoke(self):
        pass


