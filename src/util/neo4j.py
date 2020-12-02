import os
from neo4j import GraphDatabase, basic_auth


class NeoDriver(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def write(self, query, **kwargs):
        with self._driver.session() as session:
            return session.write_transaction(self.run, query, **kwargs)

    def read(self, query, **kwargs):
        with self._driver.session() as session:
            return session.read_transaction(self.run, query, **kwargs)

    @staticmethod
    def run(tx, query, **kwargs):
        result = tx.run(query, **kwargs)
        values = result.data()
        result.consume()
        return values