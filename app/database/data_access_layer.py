import uuid
from datetime import datetime
from typing import Dict, Any

from neo4j import GraphDatabase

from config.settings import Config


class Neo4jHandler:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query: str, parameters: dict[str, any] = None):
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self._run_query, query, parameters)
                return result
            except Exception as e:
                raise

    @staticmethod
    def _run_query(tx, query: str, parameters: dict[str, any]):
        # Run the query and collect all the results into a list before closing the transaction
        result = tx.run(query, parameters or {})
        return [record.data() for record in result]  # This ensures we collect data before transaction closes
conf=Config()
uri = conf.NEO4J_URI
user = conf.NEO4J_USER
password = conf.NEO4J_PASSWORD
folder_path = "../../data/Belgium-Flanders/json"  # Replace with the actual folder path

neo4j_handler = Neo4jHandler(uri, user, password)

class DataAccessLayer:
    def __init__(self, neo4j_handler: Neo4jHandler):
        self.neo4j_handler = neo4j_handler

    # Get statements based on optional filters
    def get_statements(
        self,
        filters: dict[str, any] = {},
        similarity_weight: float = 1.0,  # Adjusted default weight
        preference_weight: float = 0.2,  # New parameter for preferences
        recency_weight: float = 0.2,  # Adjusted to maintain total weight <=1
        limit: int = 10,
        skip: int = 0,
        include_prev_next: bool = False
    ) -> list[dict]:
        """
        Fetch statements based on optional filters, including the ability to filter by meeting names, meeting IDs,
        section IDs, and statement IDs. Optionally sorted by a weighted combination of similarity, preferences similarity, and recency.
        Can also include previous and next statements relative to each statement.

        :param filters: Dictionary of filters which can include:
                        - 'meetings': List of meeting names to filter.
                        - 'meeting_ids': List of meeting IDs to filter.
                        - 'statement_ids': List of statement IDs to filter.
                        - 'parties': List of political parties to filter.
                        - 'startdate': Start date (inclusive) for meeting dates.
                        - 'enddate': End date (inclusive) for meeting dates.
                        - 'sections': List of section IDs to filter.
                        - 'vector': Vector for similarity comparison.
                        - 'preferences': Vector for preferences similarity comparison.
        :param similarity_weight: Weight for the primary similarity score (default=1.0).
        :param preference_weight: Weight for the preferences similarity score (default=0.2).
        :param recency_weight: Weight for the recency score (default=0.2).
        :param limit: Maximum number of results to return (default=10).
        :param skip: Number of results to skip for pagination (default=0).
        :param include_prev_next: If True, include previous and next statements for each statement.
        :return: List of dictionaries containing statement details, with optional previous and next statements.
        """

        query_parts = [
            "MATCH (sec:Section)<-[:HAS_SECTION]-(m:Meeting) ",
            "MATCH (s:Statement)-[:MADE_DURING]->(m) ",
            "WHERE 1=1 "
        ]

        main_filters = {}

        # Apply filters based on the provided dictionary
        if 'meetings' in filters and filters['meetings']:
            query_parts.append(" AND m.name IN $meetings ")
            main_filters['meetings'] = filters['meetings']

        if 'meeting_ids' in filters and filters['meeting_ids']:
            query_parts.append(" AND m.id IN $meeting_ids ")
            main_filters['meeting_ids'] = filters['meeting_ids']

        if 'statement_ids' in filters and filters['statement_ids']:
            query_parts.append(" AND s.id IN $statement_ids ")
            main_filters['statement_ids'] = filters['statement_ids']

        if 'parties' in filters and filters['parties']:
            query_parts.append(" AND s.party IN $parties ")
            main_filters['parties'] = filters['parties']

        if 'startdate' in filters and filters['startdate']:
            query_parts.append(" AND m.date >= $startdate ")
            main_filters['startdate'] = filters['startdate']

        if 'enddate' in filters and filters['enddate']:
            query_parts.append(" AND m.date <= $enddate ")
            main_filters['enddate'] = filters['enddate']

        if 'sections' in filters and filters['sections']:
            query_parts.append(" AND sec.id IN $sections ")
            main_filters['sections'] = filters['sections']

        # OPTIONAL MATCH for Person
        query_parts.append("OPTIONAL MATCH (p:Person)-[:MADE]->(s) ")
        query_parts.append("MATCH (s)-[:IS_ABOUT]->(sec) ")

        # Handle vector and preferences for similarity calculations
        if 'vector' in filters or 'preferences' in filters:
            query_parts.append("""
                WITH s, p, m, sec,
                     CASE WHEN $vector IS NOT NULL THEN gds.similarity.cosine(s.vector, $vector) ELSE 0 END AS similarity,
                     CASE WHEN $preferences IS NOT NULL THEN gds.similarity.cosine(s.vector, $preferences) ELSE 0 END AS preference_similarity,
                     duration.inDays(m.date, date()).days AS daysSinceMeeting

                // Convert days-since-meeting into a "recency" score
                WITH s, p, m, sec, similarity, preference_similarity,
                     CASE WHEN daysSinceMeeting < 0
                          THEN 1.0
                          ELSE 1.0 / (1 + toFloat(daysSinceMeeting))
                     END AS recency

                // Calculate weighted combination using parameters
                WITH s, p, m, sec, similarity, preference_similarity, recency,
                     ($similarity_weight * similarity +
                      $preference_weight * preference_similarity +
                      $recency_weight * recency) AS combinedScore

                ORDER BY combinedScore DESC
                LIMIT $limit
            """)
        else:
            # If no vectors are provided, order by meeting date DESC as a fallback
            query_parts.append(" ORDER BY m.date DESC   LIMIT $limit ")

        # If include_prev_next is True, add OPTIONAL MATCH clauses to find previous and next statements
        if include_prev_next:
            query_parts.append("""
                WITH s, p, m, sec
                OPTIONAL MATCH (prev:Statement)-[:HAS_NEXT]->(s)
                OPTIONAL MATCH (s)-[:HAS_NEXT]->(next:Statement)
            """)

        # Build final query
        query = ''.join(query_parts) + """
            RETURN s.statement AS statement,
                   s.person      AS person,
                   m.name        AS meeting,
                   m.id          AS meeting_id,
                   m.date        AS meetingDate,
                   s.party       AS political_party,
                   sec.topic     AS section_topic,
                   COLLECT(DISTINCT s.id) AS statement_id
        """

        if include_prev_next:
            # Append previous and next statement information to RETURN
            query += """,
                   prev.statement AS previous_statement,
                   next.statement AS next_statement
            """

        # Prepare parameters
        parameters = {
            'meetings': filters.get('meetings'),
            'meeting_ids': filters.get('meeting_ids'),
            'statement_ids': filters.get('statement_ids'),
            'parties': filters.get('parties'),
            'startdate': filters.get('startdate'),
            'enddate': filters.get('enddate'),
            'sections': filters.get('sections'),
            'vector': filters.get('vector'),
            'preferences': filters.get('preferences'),  # New parameter for preferences
            # Weighted combination parameters
            'similarity_weight': similarity_weight,
            'preference_weight': preference_weight,  # New parameter
            'recency_weight': recency_weight,  # Adjusted parameter
            'limit': limit,  # Added limit to parameters
            'skip': skip  # Added skip to parameters
        }

        # Remove keys with None or empty values to prevent Neo4j errors
        parameters = {k: v for k, v in parameters.items() if v}

        # **Optional Debugging: Log the Query and Parameters**
        # Uncomment the following lines to print the query and parameters for debugging purposes.
        # print("Generated Query:")
        # print(query)
        # print("Parameters:")
        # print(parameters)

        # Execute query with parameters
        result = self.neo4j_handler.execute_query(query, parameters)
        for row in result:
            if 'meetingDate' in row and row['meetingDate']:
                row['meetingDate'] = str(row['meetingDate'])
        return result

    def get_relevant_sections(
            self,
            meeting_ids: list[str],
            vector: list[float],
            preferences: list[float],
            similarity_weight: float = 1.0,  # Weight for similarity
            preference_weight: float = 0.2,  # Weight for preferences similarity
            limit: int = 10,
            skip: int = 0
    ) -> list[dict]:
        """
        Retrieve the most relevant sections for the given meeting IDs based on similarity and preferences vectors.

        :param meeting_ids: A single meeting ID or a list of meeting IDs to filter sections.
        :param vector: Vector for primary similarity comparison.
        :param preferences: Vector for preferences similarity comparison.
        :param similarity_weight: Weight for the primary similarity score (default=1.0).
        :param preference_weight: Weight for the preferences similarity score (default=0.2).
        :param limit: Maximum number of sections to return (default=10).
        :param skip: Number of results to skip for pagination (default=0).
        :return: List of dictionaries containing section details ordered by relevance.
        """

        # Ensure meeting_ids is a list
        if isinstance(meeting_ids, str):
            meeting_ids = [meeting_ids]

        query = """
            MATCH (sec:Section)-[:BELONGS_TO]->(m:Meeting)
            WHERE m.id IN $meeting_ids
            WITH sec, m
            // Calculate similarity scores
            WITH sec,
                 CASE WHEN $vector IS NOT NULL THEN gds.similarity.cosine(sec.vector, $vector) ELSE 0 END AS similarity,
                 CASE WHEN $preferences IS NOT NULL THEN gds.similarity.cosine(sec.vector, $preferences) ELSE 0 END AS preference_similarity
            // Calculate combined relevance score
            WITH sec, similarity, preference_similarity,
                 ($similarity_weight * similarity +
                  $preference_weight * preference_similarity) AS combinedScore
            // Order by combined score descending
            ORDER BY combinedScore DESC
            SKIP $skip
            LIMIT $limit
            RETURN sec.id AS section_id,
                   sec.topic AS section_topic,
                   combinedScore
        """

        parameters = {
            'meeting_ids': meeting_ids,
            'vector': vector,
            'preferences': preferences,
            'similarity_weight': similarity_weight,
            'preference_weight': preference_weight,
            'limit': limit,
            'skip': skip
        }

        # Remove keys with None values to prevent Neo4j errors
        parameters = {k: v for k, v in parameters.items() if v is not None}

        # **Optional Debugging: Log the Query and Parameters**
        # Uncomment the following lines to print the query and parameters for debugging purposes.
        # print("Generated Query:")
        # print(query)
        # print("Parameters:")
        # print(parameters)

        # Execute query with parameters
        result = self.neo4j_handler.execute_query(query, parameters)

        # Process and return the result
        sections = []
        for record in result:
            section = {
                'section_id': record.get('section_id'),
                'section_topic': record.get('section_topic'),
                'combined_score': record.get('combinedScore')
            }
            sections.append(section)

        return sections

    def get_unique_meeting_ids(
            self,
            filters: dict[str, any] = {},
            similarity_weight: float = 1.0,  # Default weight for similarity
            preference_weight: float = 0.2,  # Weight for preferences similarity
            recency_weight: float = 0.8,  # Weight for recency
            limit: int = 50,
            skip: int = 0
    ) -> list[str]:
        """
        Retrieve unique meeting IDs for statements that are relevant and recent based on the provided filters and weights.

        :param filters: Dictionary of filters (meetings, statement_ids, parties, startdate, enddate, sections, vector, preferences, etc.)
        :param similarity_weight: Weight for the primary similarity score (default=1.0).
        :param preference_weight: Weight for the preferences similarity score (default=0.2).
        :param recency_weight: Weight for the recency score (default=0.2).
        :param limit: Maximum number of unique meeting IDs to return (default=10).
        :param skip: Number of results to skip for pagination (default=0).
        :return: List of unique meeting IDs.
        """

        query_parts = [
            "MATCH (sec:Section)<-[:HAS_SECTION]-(m:Meeting) ",
            "MATCH (s:Statement)-[:MADE_DURING]->(m) ",
            "WHERE 1=1 "
        ]

        main_filters = {}

        # Apply filters based on the provided dictionary
        if filters.get('meetings'):
            query_parts.append(" AND m.name IN $meetings ")
            main_filters['meetings'] = filters.get('meetings')

        if filters.get('statement_ids'):
            query_parts.append(" AND s.id IN $statement_ids ")
            main_filters['statement_ids'] = filters.get('statement_ids')

        if filters.get('parties'):
            query_parts.append(" AND s.party IN $parties ")
            main_filters['parties'] = filters.get('parties')

        if filters.get('startdate'):
            query_parts.append(" AND m.date >= $startdate ")
            main_filters['startdate'] = filters.get('startdate')

        if filters.get('enddate'):
            query_parts.append(" AND m.date <= $enddate ")
            main_filters['enddate'] = filters.get('enddate')

        if filters.get('sections'):
            query_parts.append(" AND sec.id IN $sections ")
            main_filters['sections'] = filters.get('sections')

        # OPTIONAL MATCH for Person (if needed for additional filters)
        query_parts.append("OPTIONAL MATCH (p:Person)-[:MADE]->(s) ")
        query_parts.append("MATCH (s)-[:IS_ABOUT]->(sec) ")

        # Handle vector and preferences for similarity calculations
        if filters.get('vector') or filters.get('preferences'):
            primary_vector = filters.get('vector')
            preferences_vector = filters.get('preferences')

            query_parts.append("""
                WITH s, p, m, sec,
                     CASE WHEN $vector IS NOT NULL THEN gds.similarity.cosine(s.vector, $vector) ELSE 0 END AS similarity,
                     CASE WHEN $preferences IS NOT NULL THEN gds.similarity.cosine(s.vector, $preferences) ELSE 0 END AS preference_similarity,
                     duration.inDays(m.date, date()).days AS daysSinceMeeting

                // Convert days-since-meeting into a "recency" score
                WITH s, p, m, sec, similarity, preference_similarity,
                     CASE WHEN daysSinceMeeting < 0
                          THEN 1.0
                          ELSE 1.0 / (1 + toFloat(daysSinceMeeting))
                     END AS recency

                // Calculate weighted combination using parameters
                WITH s, p, m, sec, similarity, preference_similarity, recency,
                     ($similarity_weight * similarity +
                      $preference_weight * preference_similarity +
                      $recency_weight * recency) AS combinedScore

                ORDER BY combinedScore DESC
                LIMIT $limit
            """)
        else:
            # If no vectors are provided, order by meeting date DESC as a fallback
            query_parts.append(" ORDER BY m.date DESC LIMIT $limit ")

        # Build final query to return unique meeting IDs
        query = ''.join(query_parts) + """
            RETURN DISTINCT m.id AS meeting_id
        """

        # Prepare parameters
        parameters = {
            'meetings': filters.get('meetings'),
            'statement_ids': filters.get('statement_ids'),
            'parties': filters.get('parties'),
            'startdate': filters.get('startdate'),
            'enddate': filters.get('enddate'),
            'sections': filters.get('sections'),
            'vector': filters.get('vector'),
            'preferences': filters.get('preferences'),  # Preferences vector
            'similarity_weight': similarity_weight,
            'preference_weight': preference_weight,
            'recency_weight': recency_weight,
            'limit': limit,
            'skip': skip
        }

        # Remove keys with None values to prevent Neo4j errors
        parameters = {k: v for k, v in parameters.items() if v is not None}

        # **Optional Debugging: Log the Query and Parameters**
        # Uncomment the following lines to print the query and parameters for debugging purposes.
        # print("Generated Query:")
        # print(query)
        # print("Parameters:")
        # print(parameters)

        # Execute query with parameters
        result = self.neo4j_handler.execute_query(query, parameters)

        # Extract meeting IDs from the result
        meeting_ids = [record['meeting_id'] for record in result if 'meeting_id' in record and record['meeting_id']]

        return meeting_ids

    def run_query(self,query):
        result = self.neo4j_handler.execute_query(query, {})
        return result

    # Get next statement based on current statement ID
    # Inside your DataAccessLayer class
    # Like a statement
    def like_statement(self, user_id: str, statement_id: str):
        query = """
        MATCH (u:User {id: $user_id}), (s:Statement {id: $statement_id})
        OPTIONAL MATCH (u)-[r:DISLIKES]->(s)  // Match existing DISLIKES relationship
        DELETE r  // Remove DISLIKES relationship if exists
        MERGE (u)-[r2:LIKES]->(s)  // Create or update LIKES relationship
        ON CREATE SET r2.created_at = datetime()
        ON MATCH SET r2.updated_at = datetime()
        """
        self.neo4j_handler.execute_query(query, {'user_id': user_id, 'statement_id': statement_id})

        # Dislike a statement

    def dislike_statement(self, user_id: str, statement_id: str):
        query = """
        MATCH (u:User {id: $user_id}), (s:Statement {id: $statement_id})
        OPTIONAL MATCH (u)-[r:LIKES]->(s)  // Match existing LIKES relationship
        DELETE r  // Remove LIKES relationship if exists
        MERGE (u)-[r2:DISLIKES]->(s)  // Create or update DISLIKES relationship
        ON CREATE SET r2.created_at = datetime()
        ON MATCH SET r2.updated_at = datetime()
        """
        self.neo4j_handler.execute_query(query, {'user_id': user_id, 'statement_id': statement_id})

        # Get likes and dislikes for a statement

    def get_statement_likes_dislikes(self, statement_id: str):
        query = """
          MATCH (s:Statement {id: $statement_id})
          OPTIONAL MATCH (s)<-[likes:LIKES]-()
          OPTIONAL MATCH (s)<-[dislikes:DISLIKES]-()
          RETURN COUNT(likes) AS likes, COUNT(dislikes) AS dislikes
          """
        result = self.neo4j_handler.execute_query(query, {'statement_id': statement_id})
        return result[0] if result else {'likes': 0, 'dislikes': 0}

        # Get user reaction for a statement

    def get_user_reaction(self, user_id: str, statement_id: str):
        query = """
          MATCH (u:User {id: $user_id})-[r]->(s:Statement {id: $statement_id})
          RETURN type(r) AS reaction
          """
        result = self.neo4j_handler.execute_query(query, {'user_id': user_id, 'statement_id': statement_id})
        return result[0]['reaction'] if result else None

        # Get next statement with likes/dislikes

    def get_next(self, statement_id: str):
        query = """
          MATCH (s:Statement {id: $statement_id})-[:HAS_NEXT]->(next:Statement)
          OPTIONAL MATCH (next)<-[likes:LIKES]-()
          OPTIONAL MATCH (next)<-[dislikes:DISLIKES]-()
          OPTIONAL MATCH (p:Person)-[:MADE]->(next)
          OPTIONAL MATCH (p)-[:PART_OF]->(party:PoliticalParty)
          RETURN next.statement AS statement,
                 next.person AS person,
                 next.id AS statement_id,
                 next.party AS party, 
                 COUNT(likes) AS likes,
                 COUNT(dislikes) AS dislikes
          """
        return self.neo4j_handler.execute_query(query, {'statement_id': statement_id})

        # Get current statement with likes/dislikes

    def get_current(self, statement_id: str):
        query = """
          MATCH (s:Statement {id: $statement_id})
          OPTIONAL MATCH (s)<-[likes:LIKES]-()
          OPTIONAL MATCH (s)<-[dislikes:DISLIKES]-()
          OPTIONAL MATCH (p:Person)-[:MADE]->(s)
          OPTIONAL MATCH (p)-[:PART_OF]->(party:PoliticalParty)
          RETURN s.statement AS statement,
                 s.person AS person,
                 s.id AS statement_id,
                 s.party AS party, 
                 COUNT(likes) AS likes,
                 COUNT(dislikes) AS dislikes
          """
        return self.neo4j_handler.execute_query(query, {'statement_id': statement_id})

        # Get previous statement with likes/dislikes

    def get_previous(self, statement_id: str):
        query = """
          MATCH (s:Statement)-[:HAS_NEXT]->(prev:Statement {id: $statement_id})
          OPTIONAL MATCH (s)<-[likes:LIKES]-()
          OPTIONAL MATCH (s)<-[dislikes:DISLIKES]-()
          OPTIONAL MATCH (p:Person)-[:MADE]->(s)
          OPTIONAL MATCH (p)-[:PART_OF]->(party:PoliticalParty)
          RETURN prev.statement AS statement,
                 prev.person AS person,
                 s.id AS statement_id,
                 prev.party AS party, 
                 COUNT(likes) AS likes,
                 COUNT(dislikes) AS dislikes
          """
        return self.neo4j_handler.execute_query(query, {'statement_id': statement_id})
    # Get meetings based on optional filters
    def get_meeting(self, filters: dict[str, any] = {}):
        query_parts = [
            "MATCH (m:Meeting) ",
            "WHERE 1=1"
        ]

        # Collect dynamic conditions based on provided filters
        if meeting_ids := filters.get('meeting_ids'):
            query_parts.append(" AND m.id IN $meeting_ids")
        if meeting_names := filters.get('meeting_names'):
            query_parts.append(" AND m.name IN $meeting_names")
        if startdate := filters.get('startdate'):
            query_parts.append(" AND m.date >= $startdate")
        if enddate := filters.get('enddate'):
            query_parts.append(" AND m.date <= $enddate")

        # Ensure the final query has a space before RETURN
        query = ''.join(query_parts) + """
               RETURN m.id AS meeting_id, m.name AS meeting_name, m.date AS meeting_date
           """

        parameters = {
            'meeting_ids': filters.get('meeting_ids'),
            'meeting_names': filters.get('meeting_names'),
            'startdate': filters.get('startdate'),
            'enddate': filters.get('enddate')
        }

        result = self.neo4j_handler.execute_query(query, parameters)
        for row in result:
            if 'meeting_date' in row and row['meeting_date']:
                row['meeting_date'] = str(row['meeting_date'])
        return result

    # Get votes based on optional filters
    # Get votes based on optional filters
    def get_votes(self, filters: dict[str, any] = {}):
        query_parts = [
            "MATCH (p:Person)-[v]->(vote:Vote) ",
            "MATCH (m:Meeting)<-[:MADE_DURING]-(vote) ",  # Match the meeting associated with the vote
            "WHERE 1=1"
        ]

        # Collect dynamic conditions based on provided filters
        if vote_types := filters.get('vote_types'):
            query_parts.append(" AND type(v) IN $vote_types")  # Filtering by vote types (YES, NO, ABSTAINED)
        if persons := filters.get('persons'):
            query_parts.append(" AND p.name IN $persons")
        if parties := filters.get('parties'):  # New filter for parties
            query_parts.append(" AND p.party IN $parties")

        # Ensure the final query has a space before RETURN
        query = ''.join(query_parts) + """
               RETURN p.name AS person_name, type(v) AS vote_type, vote.id AS vote_id, 
                      m.name AS meeting_name, p.party AS party, m.date AS meeting_date
           """

        parameters = {
            'vote_types': filters.get('vote_types'),
            'persons': filters.get('persons'),
            'parties': filters.get('parties')  # Pass the parties parameter
        }

        result = self.neo4j_handler.execute_query(query, parameters)
        for row in result:
            if 'meeting_date' in row and row['meeting_date']:
                row['meeting_date'] = str(row['meeting_date'])
        return result

    def get_all_parties(self):
        query = """
        MATCH (p:Person)
        RETURN DISTINCT p.party AS party
        """
        return self.neo4j_handler.execute_query(query, {})

    def create_or_update_user(self, email: str, name: str, provider: str, provider_id: str):
        query = """
        MERGE (u:User {email: $email})
        ON CREATE SET u.id = $user_id, u.name = $name, u.created_at = datetime()
        ON MATCH SET u.name = $name, u.updated_at = datetime()
        SET u.provider = $provider, u.provider_id = $provider_id
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        user_id = str(uuid.uuid4())  # Generate a unique ID for new users
        parameters = {
            'email': email,
            'name': name,
            'provider': provider,
            'provider_id': provider_id,
            'user_id': user_id
        }
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_user_by_email(self, email: str):
        query = """
        MATCH (u:User {email: $email})
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'email': email}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_user_by_provider_id(self, provider: str, provider_id: str):
        query = """
        MATCH (u:User {provider: $provider, provider_id: $provider_id})
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'provider': provider, 'provider_id': provider_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def update_user(self, user_id: str, updates: dict):
        set_clause = ', '.join([f"u.{key} = ${key}" for key in updates.keys()])
        query = f"""
        MATCH (u:User {{id: $user_id}})
        SET {set_clause}, u.updated_at = datetime()
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'user_id': user_id, **updates}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def delete_user(self, user_id: str):
        query = """
        MATCH (u:User {id: $user_id})
        DELETE u
        RETURN count(u) AS deleted_count
        """
        parameters = {'user_id': user_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0]['deleted_count'] if result else 0

    def create_or_update_user(self, email: str, name: str, provider: str, provider_id: str):
        query = """
        MERGE (u:User {email: $email})
        ON CREATE SET u.id = $user_id, u.name = $name, u.created_at = $timestamp
        ON MATCH SET u.name = $name, u.updated_at = $timestamp
        SET u.provider = $provider, u.provider_id = $provider_id
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        user_id = str(uuid.uuid4())  # Generate a unique ID for new users
        timestamp = datetime.utcnow().isoformat()
        parameters = {
            'email': email,
            'name': name,
            'provider': provider,
            'provider_id': provider_id,
            'user_id': user_id,
            'timestamp': timestamp
        }
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_user_by_email(self, email: str):
        query = """
        MATCH (u:User {email: $email})
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'email': email}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_user_by_id(self, user_id: str):
        query = """
        MATCH (u:User {id: $user_id})
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'user_id': user_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_user_by_provider_id(self, provider: str, provider_id: str):
        query = """
        MATCH (u:User {provider: $provider, provider_id: $provider_id})
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        parameters = {'provider': provider, 'provider_id': provider_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def update_user(self, user_id: str, updates: dict):
        set_clause = ', '.join([f"u.{key} = ${key}" for key in updates.keys()])
        query = f"""
        MATCH (u:User {{id: $user_id}})
        SET {set_clause}, u.updated_at = $timestamp
        RETURN u.id AS id, u.email AS email, u.name AS name, u.provider AS provider
        """
        updates['timestamp'] = datetime.utcnow().isoformat()
        parameters = {'user_id': user_id, **updates}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def delete_user(self, user_id: str):
        query = """
        MATCH (u:User {id: $user_id})
        DELETE u
        RETURN count(u) AS deleted_count
        """
        parameters = {'user_id': user_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0]['deleted_count'] if result else 0

    # You can add more methods here as needed, for example:
    def get_user_settings(self, user_id: str):
        query = """
        MATCH (u:User {id: $user_id})
        RETURN u.settings AS settings
        """
        parameters = {'user_id': user_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0]['settings'] if result and 'settings' in result[0] else {}

    def update_user_settings(self, user_id: str, settings: dict):
        query = """
        MATCH (u:User {id: $user_id})
        SET u.settings = $settings, u.updated_at = $timestamp
        RETURN u.id AS id, u.email AS email, u.name AS name, u.settings AS settings
        """
        parameters = {'user_id': user_id, 'settings': settings, 'timestamp': datetime.utcnow().isoformat()}
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0] if result else None

    def get_all_conversations(self, user_id: str):
        """
        Return a list of the user's conversations, ignoring the messages for now.
        """
        query = """
        MATCH (u:User {id: $user_id})-[:HAS_CONVERSATION]->(c:Conversation)
        RETURN c.id AS id, c.title AS title, c.created_at AS created_at
        ORDER BY c.created_at DESC
        """
        params = {"user_id": user_id}
        return self.neo4j_handler.execute_query(query, params)

    def get_user_conversations_with_messages(self, user_id: str):
        """
        Return an array of { id, title, created_at, messages: [...] } objects.
        """
        conversations = self.get_all_conversations(user_id)
        output = []
        for convo in conversations:
            cid = convo["id"]
            messages = self.get_chat_messages_by_conversation(cid)
            convo["messages"] = messages
            output.append(convo)
        return output

    def get_chat_messages_with_citations(self, conversation_id: str):
        query = """
        MATCH (c:Conversation {id: $conversation_id})-[:HAS_MESSAGE]->(cm:ChatMessage)
        OPTIONAL MATCH (cm)-[:CITES]->(s:Statement)
        RETURN cm.id AS message_id,
               cm.question AS question,
               cm.answer AS answer,
               cm.created_at AS created_at,
               COLLECT(s.id) AS statements
        ORDER BY cm.created_at ASC
        """
        params = {"conversation_id": conversation_id}
        result = self.neo4j_handler.execute_query(query, params)
        for row in result:
            if 'created_at' in row and row['created_at']:
                row['created_at'] = str(row['created_at'])
        return result

    def get_statement(self, statement_id: str):
        """
        Retrieve a single statement by its ID, including related information such as
        the person who made the statement, the meeting during which it was made,
        the associated political party, and counts of likes and dislikes.

        :param statement_id: The unique identifier of the statement.
        :return: A dictionary with statement details or None if not found.
        """
        query = """
        MATCH (s:Statement {id: $statement_id})
        OPTIONAL MATCH (s)<-[:MADE_DURING]-(m:Meeting)
        OPTIONAL MATCH (s)<-[:MADE]-(p:Person)
        OPTIONAL MATCH (p)-[:PART_OF]->(party:PoliticalParty)
        OPTIONAL MATCH (s)<-[likes:LIKES]-()
        OPTIONAL MATCH (s)<-[dislikes:DISLIKES]-()
        RETURN 
            s.statement AS statement,
            p.name AS person,
            m.name AS meeting,
            m.date AS meetingDate,
            party.name AS party,
            COUNT(likes) AS likes,
            COUNT(dislikes) AS dislikes
        """

        parameters = {'statement_id': statement_id}

        result = self.neo4j_handler.execute_query(query, parameters)

        if not result:
            return None

        statement = result[0]

        # Convert meetingDate to string if it exists
        if 'meetingDate' in statement and statement['meetingDate']:
            statement['meetingDate'] = str(statement['meetingDate'])

        return statement

    def get_chat_messages_by_conversation(self, conversation_id: str):
        """
        Return chat messages (question/answer) for this conversation,
        sorted by created_at ascending or descending as needed.
        """
        query = """
        MATCH (c:Conversation {id: $conversation_id})-[:HAS_MESSAGE]->(cm:ChatMessage)
        RETURN cm.id AS id, cm.question AS question, cm.answer AS answer, cm.created_at AS created_at
        ORDER BY cm.created_at ASC
        """
        params = {"conversation_id": conversation_id}
        result = self.neo4j_handler.execute_query(query, params)
        for row in result:
            if 'created_at' in row and row['created_at']:
                row['created_at'] = str(row['created_at'])
        return  result

    def link_message_to_citations(self, message_id: str, statement_ids: list[str]):
        query = """
        MATCH (m:ChatMessage {id: $message_id})
        UNWIND $statement_ids AS statement_id
        MATCH (s:Statement {id: statement_id})
        MERGE (m)-[:CITES]->(s)
        """
        parameters = {"message_id": message_id, "statement_ids": statement_ids}
        result = self.neo4j_handler.execute_query(query, parameters)
        print(f"Created CITES relationships for message {message_id}: {statement_ids}")  # Debugging log
        return result
    def save_chat_message(self, conversation_id: str, question: str, answer: str):
        """
        Create a ChatMessage node in Neo4j, linking it via
        (c:Conversation)-[:HAS_MESSAGE]->(cm:ChatMessage).
        """
        query = """
        MATCH (c:Conversation {id: $conversation_id})
        CREATE (cm:ChatMessage {
            id: apoc.create.uuid(),
            question: $question,
            answer: $answer,
            created_at: datetime()
        })
        CREATE (c)-[:HAS_MESSAGE]->(cm)
        RETURN cm.id AS chat_message_id
        """
        parameters = {
            "conversation_id": conversation_id,
            "question": question,
            "answer": answer
        }
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0]["chat_message_id"] if result else None

    def get_all_chat_messages_for_user(self, user_id: str):
        query = """
        MATCH (u:User {id: $user_id})-[:HAS_CONVERSATION]->(c:Conversation)-[:HAS_MESSAGE]->(m:ChatMessage)
        RETURN
          c.id AS conversation_id,
          c.title AS conversation_title,
          m.question AS question,
          m.answer AS answer,
          m.created_at AS created_at
        ORDER BY m.created_at DESC
        """
        params = {"user_id": user_id}
        result = self.neo4j_handler.execute_query(query, params)

        # Convert Neo4j DateTime to string if needed
        for row in result:
            if 'created_at' in row and row['created_at']:
                row['created_at'] = str(row['created_at'])

        return result

    def create_conversation(self, user_id: str, title: str) -> str:
        """
        Creates a new conversation node, linking it to the user.
        Returns the conversation_id (UUID).
        """
        conversation_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        query = """
        MATCH (u:User {id: $user_id})
        CREATE (c:Conversation {
            id: $conversation_id,
            created_at: $timestamp,
            title: $title
        })
        CREATE (u)-[:HAS_CONVERSATION]->(c)
        RETURN c.id AS conversation_id
        """
        parameters = {
            "user_id": user_id,
            "conversation_id": conversation_id,
            "timestamp": timestamp,
            "title": title
        }
        result = self.neo4j_handler.execute_query(query, parameters)
        return result[0]["conversation_id"] if result else None

    def get_conversation(self, conversation_id: str, user_id: str):
        """
        Returns the conversation node if it belongs to the given user.
        Otherwise returns None.
        """
        query = """
        MATCH (u:User {id: $user_id})-[:HAS_CONVERSATION]->(c:Conversation {id: $conversation_id})
        RETURN c.id AS id, c.title AS title, c.created_at AS created_at
        """
        params = {"conversation_id": conversation_id, "user_id": user_id}
        result = self.neo4j_handler.execute_query(query, params)
        return result[0] if result else None

    def get_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieves the user's interests and personalization weight.
        """
        query = """
        MATCH (u:User {id: $user_id})
        RETURN u.interests AS interests, u.personalization_weight AS personalization_weight
        """
        parameters = {"user_id": user_id}
        result = self.neo4j_handler.execute_query(query, parameters)
        if result and 'interests' in result[0] and 'personalization_weight' in result[0]:
            return {
                "interests": result[0]['interests'] or [],
                "personalization_weight": result[0]['personalization_weight'] or 50  # Default value
            }
        else:
            # Return default settings if properties don't exist
            return {
                "interests": [],
                "personalization_weight": 50
            }

    def update_user_preferences(self, user_id: str, preferences: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates the user's interests and personalization weight.
        """
        query = """
        MATCH (u:User {id: $user_id})
        SET 
            u.interests = $preferences.interests,
            u.personalization_weight = $preferences.personalization_weight,
            u.updated_at = datetime()
        RETURN u.interests AS interests, u.personalization_weight AS personalization_weight
        """
        parameters = {
            "user_id": user_id,
            "preferences": preferences
        }
        result = self.neo4j_handler.execute_query(query, parameters)
        if result and 'interests' in result[0] and 'personalization_weight' in result[0]:
            return {
                "interests": result[0]['interests'] or [],
                "personalization_weight": result[0]['personalization_weight'] or 50
            }
        else:
            # Return default settings if update fails
            return {
                "interests": [],
                "personalization_weight": 50
            }
    def get_user_daily_message_count(self, user_id: str) -> int:
        """
        Returns the count of messages the user has sent today.
        """
        query = """
        MATCH (u:User {id: $user_id})-[:HAS_CONVERSATION]->(c:Conversation)-[:HAS_MESSAGE]->(m:ChatMessage)
        WHERE date(m.created_at) = date()  // Compare the date portion of m.created_at to today's date
        RETURN COUNT(m) AS message_count
        """
        params = {"user_id": user_id}
        result = self.neo4j_handler.execute_query(query, params)

        # If there is a matching record, return that count, otherwise 0
        return result[0]["message_count"] if result else 0