import os
import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from point_in_time import Base, Record, Repository


class ExchangeRepositoryTest(unittest.TestCase):
    engine = create_engine(os.environ['DATABASE_URL'])
    Session = sessionmaker(bind=engine)
    session = Session()

    def setUp(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.repo = Repository(self.session)

    def test_update(self):
        record = Record.build('code', 'name')
        self.repo.update(record)
