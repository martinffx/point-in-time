import uuid
from datetime import datetime
from sqlalchemy import Column, DateTime, String, and_
from sqlalchemy.ext.declarative import declarative_base
from functools import reduce

LAST_VALID = datetime.strptime('3000-01-01 UTC', '%Y-%m-%d %Z')
Base = declarative_base()


class PointInTimeMixin(object):
    first_valid = Column(DateTime(True), nullable=False)
    last_valid = Column(DateTime(True), primary_key=True, nullable=False)
    version_uuid = Column(String(32), nullable=False)

    __mapper_args__ = {
        'version_id_col': version_uuid,
        'version_id_generator': lambda version: uuid.uuid4().hex
    }

    def hash_value(self):
        return hash(self.value())


class Record(Base, PointInTimeMixin):
    __table_name__ = 'record'

    code = Column(String(10), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)

    def key(self):
        return self.code

    def value(self):
        return self.name

    def select(self, session):
        return session.query(Record).filter(
            and_(Record.code == self.code, Record.last_valid == LAST_VALID))


class Repository:
    def __init__(self, session):
        self.session = session

    def update(self, item):
        self.update_all([item])

    def update_all(self, items):
        """Point in time update list of exchanges"""
        data = dict([(item.key(), item) for item in items])
        first = items.pop()

        # build up a single query that is a union of all the
        # queries for indvidual records
        query = reduce(
            lambda query, item: query.union(item.select(self.session)), items,
            first.select(self.session))

        # check if the value of the updated data is different from what is
        # stored in the DB and invalidate the previous record
        prev = list(
            map(lambda x: x.invalid(),
                filter(lambda x: self.__changed(data[x.key()], x),
                       query.all())))

        self.session.add_all(prev)
        self.session.add_all(data.values())
        self.session.commit()

    def __changed(self, cur, prev):
        return cur.hash_value() == prev.hash_value()
