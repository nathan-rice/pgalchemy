import sqlalchemy as sa
from sqlalchemy import Text
from sqlalchemy.schema import CreateTable
from pgalchemy import domain as d


class EmailAddressDomain(d.Domain, Text):
    name = "email_address"
    constraint = d.VALUE.like("%@gmail.com") & (d.VALUE != "jerk@gmail.com")


md = sa.MetaData()
table = sa.Table("test_table", md,
                 sa.Column("id", sa.Integer, primary_key=True),
                 sa.Column("email", EmailAddressDomain))

print(CreateTable(table))
print(EmailAddressDomain.constraint.compile())