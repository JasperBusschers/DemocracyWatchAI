import unittest
from app.database.DataAccessLayer import DataAccessLayer

class TestDALRealDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize DataAccessLayer
        cls.dal = DataAccessLayer()

    @classmethod
    def tearDownClass(cls):
        # Close the database connection
        cls.dal.close()
    def setUp(self):
        # Create test data before each test
        with self.dal.driver.session() as session:
            session.write_transaction(self.dal._create_test_data)

    def tearDown(self):
        # Clean up the database after each test
        self.clean_database()

    def clean_database(self):
        # This method removes all nodes and relationships from the database
        with self.dal.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def test_add_entity(self):
        # Add a new entity and verify it exists
        self.dal.add_entity("Country", {"name": "Atlantis"})
        result = self.dal.find_entity("Country", "name", "Atlantis")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], "Atlantis")

    def test_find_entity(self):
        # Find an existing entity
        result = self.dal.find_entity("Representative", "name", "John Doe")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], "John Doe")

    def test_find_similar_entities(self):
        # Test finding similar entities based on embedding similarity
        embedding = [0.1, -0.2, 0.3]
        similar_topics = self.dal.find_similar_entities("Topic", "name", embedding)
        self.assertTrue(any(topic for topic, _ in similar_topics if topic == 'Global Warming'))

if __name__ == '__main__':
    unittest.main()
