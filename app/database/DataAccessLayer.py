from neo4j import GraphDatabase
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from app.database import db_config


class DataAccessLayer:
    def __init__(self):
        self.driver = GraphDatabase.driver(db_config.NEO4J_URI, auth=(db_config.NEO4J_USER, db_config.NEO4J_PASSWORD))
    @staticmethod
    def _create_test_data(tx):
        tx.run("MERGE (:Country {name: 'SampleLand'})")
        tx.run("MERGE (:PoliticalParty {name: 'GreenParty'})")
        tx.run("MERGE (:Representative {name: 'John Doe'})")
        tx.run("MERGE (:Debate {name: 'Climate Debate'})")
        tx.run("MERGE (:Topic {name: 'Global Warming', embedding: [0.1, -0.2, 0.3]})")
        tx.run("MERGE (:Statement {text: 'Climate change is real.', embedding: [0.2, -0.1, 0.4]})")
        tx.run("MERGE (:Vote {type: 'Yes'})")

    def close(self):
        self.driver.close()

    def add_entity(self, label, properties):
        with self.driver.session() as session:
            session.write_transaction(self._add_entity, label, properties)

    @staticmethod
    def _add_entity(tx, label, properties):
        props_string = ", ".join([f"{key}: ${key}" for key in properties])
        query = f"MERGE (n:{label} {{{props_string}}})"
        tx.run(query, **properties)

    def find_entity(self, label, property_key, property_value):
        with self.driver.session() as session:
            result = session.execute_read(self._find_entity, label, property_key, property_value)
            return result

    @staticmethod
    def _find_entity(tx, label, property_key, property_value):
        query = f"MATCH (n:{label} {{{property_key}: $property_value}}) RETURN n"
        results = tx.run(query, property_value=property_value)
        return [record['n'] for record in results]

    def find_similar_entities(self, label, entity_property, embedding):
        with self.driver.session() as session:
            result = session.execute_read(self._find_similar_entities, label, entity_property, embedding)
            return result

    @staticmethod
    def _find_similar_entities(tx, label, entity_property, target_embedding):
        target_embedding = np.array(target_embedding).reshape(1, -1)
        query = f"MATCH (n:{label}) RETURN n.{entity_property} AS name, n.embedding AS embedding"
        records = tx.run(query)
        similar_entities = []
        for record in records:
            embedding = np.array(record['embedding']).reshape(1, -1)
            similarity = cosine_similarity(target_embedding, embedding)[0][0]
            similar_entities.append((record['name'], similarity))
        similar_entities.sort(key=lambda x: x[1], reverse=True)
        return similar_entities


