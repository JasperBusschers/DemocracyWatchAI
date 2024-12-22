import uuid
from datetime import datetime

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
    def get_statements(self, filters: dict[str, any] = {}):
        query_parts = [
            "MATCH (sec:Section)<-[:HAS_SECTION]-(m:Meeting) ",
            "WHERE 1=1"
        ]

        # Collect dynamic conditions based on provided filters
        if meetings := filters.get('meetings'):
            query_parts.append(" AND m.name IN $meetings")
        if parties := filters.get('parties'):  # New filter for parties
            query_parts.append(" AND p.party IN $parties")

        query_parts.append(" MATCH (s:Statement)-[:MADE_DURING]->(m) ")
        query_parts.append("OPTIONAL MATCH (p:Person)-[:MADE]->(s) ")

        # Additional filters
        if persons := filters.get('persons'):
            query_parts.append(" AND p.name IN $persons")
        if sections := filters.get('sections'):
            query_parts.append(" AND sec.id IN $sections")
        if startdate := filters.get('startdate'):
            query_parts.append(" AND s.date >= $startdate")
        if enddate := filters.get('enddate'):
            query_parts.append(" AND s.date <= $enddate")

        # If a vector is provided, add similarity search
        if vector := filters.get('vector'):
            query_parts.append(" WITH s, p, m, sec, gds.similarity.cosine(s.vector, $vector) AS similarity ")
            query_parts.append(" ORDER BY similarity DESC LIMIT 10 ")

        # Ensure the final query has a space before RETURN
        query = ''.join(query_parts) + """
                    RETURN s.statement AS statement,  p.name AS person, m.name AS meeting, m.date AS meetingDate, COLLECT(DISTINCT s.id) AS statement_id
               """

        parameters = {
            'persons': filters.get('persons'),
            'meetings': filters.get('meetings'),
            'sections': filters.get('sections'),
            'startdate': filters.get('startdate'),
            'enddate': filters.get('enddate'),
            'vector': filters.get('vector'),
            'parties': filters.get('parties')  # Pass the parties parameter
        }

        result = self.neo4j_handler.execute_query(query, parameters)
        return result

    # Get sections based on optional filters
    def get_sections(self, filters: dict[str, any] = {}):
        query_parts = [
            "MATCH (sec:Section) ",
            "WHERE 1=1"
        ]

        # Collect dynamic conditions based on provided filters
        if sections := filters.get('sections'):
            query_parts.append(" AND sec.id IN $sections")
        if topics := filters.get('topics'):
            query_parts.append(" AND sec.topic IN $topics")
        if startdate := filters.get('startdate'):
            query_parts.append(" AND sec.startDate >= $startdate")
        if enddate := filters.get('enddate'):
            query_parts.append(" AND sec.endDate <= $enddate")

        # If a vector is provided, add similarity search
        if vector := filters.get('vector'):
            query_parts.append(" WITH sec, gds.similarity.cosine(sec.vector, $vector) AS similarity ")
            query_parts.append(" ORDER BY similarity DESC LIMIT 5 ")

        # Ensure the final query has a space before RETURN
        query = ''.join(query_parts) + """
               RETURN sec.id AS section_id, sec.topic AS topic
           """

        parameters = {
            'sections': filters.get('sections'),
            'topics': filters.get('topics'),
            'startdate': filters.get('startdate'),
            'enddate': filters.get('enddate'),
            'vector': filters.get('vector')
        }

        result = self.neo4j_handler.execute_query(query, parameters)
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
                 p.name AS person,
                 next.id AS statement_id,
                 party.name AS party, 
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
                 p.name AS person,
                 s.id AS statement_id,
                 party.name AS party, 
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
          RETURN s.statement AS statement,
                 p.name AS person,
                 party.name AS party, 
                 s.id AS statement_id,
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
