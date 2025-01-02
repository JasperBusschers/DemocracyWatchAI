# dag_b_process.py
"""
DAG B: Processes the files downloaded by DAG A. This DAG is triggered by DAG A
using TriggerDagRunOperator, and it also demonstrates how to read from local files
on the same Airflow server, then load them into Neo4j.
"""

import os
import glob
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# 1) Import what we need from the processing module
#    (Make sure 'processing.py' is in your PYTHONPATH or in the same folder)
import os
import json
import logging
from datetime import datetime

from dotenv import load_dotenv
from neo4j import GraphDatabase
import uuid
from typing import Dict, Any, List

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Neo4jHandler:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query: str, parameters: Dict[str, Any] = None):
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self._run_query, query, parameters)
                return result
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise

    @staticmethod
    def _run_query(tx, query: str, parameters: Dict[str, Any]):
        return tx.run(query, parameters or {})

def create_meeting(neo4j_handler: Neo4jHandler, json_data: Dict[str, Any]) -> str:
    meeting_id = str(uuid.uuid4())

    # 1) Convert the date string to ISO-8601
    iso_date = parse_date_string(json_data['date'])

    # 2) Use date($date) so that Neo4j creates a Date property
    query = """
    CREATE (m:Meeting {
        id: $id,
        name: $name,
        date: date($date),
        country: $country,
        region: $region,
        youtube_link: $youtube_link,
        pdf_link: $pdf_link
    })
    """

    neo4j_handler.execute_query(query, {
        'id': meeting_id,
        'name': json_data['name'],
        'date': iso_date,   # pass the ISO string
        'country': json_data['country'],
        'region': json_data['region'],
        'youtube_link': 'https://www.youtube.com/watch?v=' + json_data['youtube_link'],
        'pdf_link': json_data['pdf_link']
    })
    return meeting_id

def create_person(neo4j_handler: Neo4jHandler, person_name: str) -> str:
    person_id = str(uuid.uuid4())
    query = """
    MERGE (p:Person {name: $name})
    ON CREATE SET p.id = $id
    RETURN p.id as id
    """
    result = neo4j_handler.execute_query(query, {
        'id': person_id,
        'name': person_name,
    })
    return result.single()['id']


def create_section(neo4j_handler: Neo4jHandler, section: Dict[str, Any], meeting_id: str) -> str:
    section_id = str(uuid.uuid4())
    query = """
    CREATE (s:Section {id: $id, type: $type, voted: $voted, vector: $vector, topic: $topic}) 
    WITH s
    MATCH (m:Meeting {id: $meeting_id})
    CREATE (m)-[:HAS_SECTION]->(s)
    """
    neo4j_handler.execute_query(query, {
        'id': section['id'],
        'type': section['type'],
        'voted': section['voted'],
        'vector': section.get('vector'),
        'meeting_id': meeting_id,
        'topic': section.get('topic'),
    })
    return section['id']


def create_started_by_relationships(neo4j_handler: Neo4jHandler, section_id: str, people: List[str]):
    for person_name in people:
        query = """
        MATCH (s:Section {id: $section_id})
        MATCH (p:Person {name: $person_name})
        MERGE (p)-[:IS_STARTED_BY]->(s)
        """
        neo4j_handler.execute_query(query, {
            'section_id': section_id,
            'person_name': person_name
        })


def create_started_by_relationship(neo4j_handler: Neo4jHandler, section_id: str, person_name: str):
    query = """
    MATCH (s:Section {id: $section_id})
    MATCH (p:Person {name: $person_name})
    MERGE (p)-[:STARTED]->(s)
    """
    neo4j_handler.execute_query(query, {
        'section_id': section_id,
        'person_name': person_name
    })


def load_attendance(neo4j_handler: Neo4jHandler, attendance_data: Dict[str, list], meeting_id: str):
    for status, people in attendance_data.items():
        relationship = {
            'present': 'ATTENDED',
            'absent_with_notice': 'DID_NOT_ATTEND',
            'absent_without_notice': 'DID_NOT_ATTEND_WITHOUT_REASON'
        }.get(status)

        if relationship:
            for person in people:
                query = f"""
                MATCH (m:Meeting {{id: $meeting_id}})
                MERGE (p:Person {{name: $person_name}})
                MERGE (p)-[:{relationship}]->(m)
                """
                neo4j_handler.execute_query(query, {
                    'meeting_id': meeting_id,
                    'person_name': person
                })


def load_statements(neo4j_handler: Neo4jHandler, statements: List[Dict[str, Any]], meeting_id: str):
    # First create all statements
    create_statements_query = """
    UNWIND $statements AS stmt
    MERGE (s:Statement {id: stmt.id})
    SET s.statement = stmt.statement,
        s.vector = stmt.vector,
        s.party = stmt.Party,
        s.person = stmt.person
    """
    neo4j_handler.execute_query(create_statements_query, {'statements': statements})

    # Create MADE relationships
    create_made_relationships_query = """
    UNWIND $statements AS stmt
    MATCH (s:Statement {id: stmt.id})
    MATCH (p:Person {name: stmt.person_ref})
    MERGE (p)-[:MADE]->(s)
    """
    neo4j_handler.execute_query(create_made_relationships_query, {'statements': statements})

    # Create IS_ABOUT relationships using UNWIND for better performance
    statements_with_topics = [
        {
            'statement_id': stmt['id'],
            'section_ids': stmt['topic']
        }
        for stmt in statements
        if 'topic' in stmt and isinstance(stmt['topic'], list)
    ]

    # Configure logging
    logging.basicConfig(level=logging.DEBUG)

    if statements_with_topics:
        create_is_about_query = """
        UNWIND $statements_with_topics AS stmt
        UNWIND stmt.section_ids AS section_id
        MATCH (s:Statement {id: stmt.statement_id})
        MATCH (sec:Section {id: section_id})
        MERGE (s)-[:IS_ABOUT]->(sec)
        """
        try:
            logging.debug(f"Executing query with data: {statements_with_topics}")
            print(statements_with_topics)
            res = neo4j_handler.execute_query(create_is_about_query, {
                'statements_with_topics': statements_with_topics
            })
            logger.info(f"Created IS_ABOUT relationships for {len(statements_with_topics)} statements")
        except Exception as e:
            logger.error(f"Error creating IS_ABOUT relationships: {e}")
            # Log some sample data to help debug
            if statements_with_topics:
                logger.error(f"Sample statement data: {statements_with_topics[0]}")

    # Create MADE_DURING relationships
    create_made_during_query = """
    UNWIND $statements AS stmt
    MATCH (s:Statement {id: stmt.id})
    MATCH (m:Meeting {id: $meeting_id})
    MERGE (s)-[:MADE_DURING]->(m)
    """
    neo4j_handler.execute_query(create_made_during_query, {
        'statements': statements,
        'meeting_id': meeting_id
    })

    # Create HAS_NEXT and HAS_PREVIOUS relationships
    create_has_next_relationships_query = """
    UNWIND $statements AS stmt
    MATCH (s:Statement {id: stmt.id})
    WITH s, stmt
    WHERE stmt.next IS NOT NULL
    MATCH (next:Statement {id: stmt.next})
    MERGE (s)-[:HAS_NEXT]->(next)
    """
    neo4j_handler.execute_query(create_has_next_relationships_query, {'statements': statements})

    create_has_previous_relationships_query = """
    UNWIND $statements AS stmt
    MATCH (s:Statement {id: stmt.id})
    WITH s, stmt
    WHERE stmt.previous IS NOT NULL
    MATCH (previous:Statement {id: stmt.previous})
    MERGE (s)-[:HAS_PREVIOUS]->(previous)
    """
    neo4j_handler.execute_query(create_has_previous_relationships_query, {'statements': statements})

    # Link Person to Political Party
    for stmt in statements:
        if 'Party' in stmt and stmt['Party'] is not None and stmt['person_ref'] is not None:
            link_person_to_party(neo4j_handler, stmt['person_ref'], stmt['Party'])

    logger.info(f"Loaded {len(statements)} statements.")


# Add this verification function
def verify_is_about_relationships(neo4j_handler: Neo4jHandler):
    """Verify that IS_ABOUT relationships were created correctly"""
    verification_query = """
    MATCH (s:Statement)-[r:IS_ABOUT]->(sec:Section)
    RETURN COUNT(r) as relationship_count
    """
    result = neo4j_handler.execute_query(verification_query)
    count = result.single()['relationship_count']
    logger.info(f"Found {count} IS_ABOUT relationships")

    # Sample some relationships for verification
    sample_query = """
    MATCH (s:Statement)-[r:IS_ABOUT]->(sec:Section)
    RETURN s.id as statement_id, sec.id as section_id
    LIMIT 5
    """
    sample_results = neo4j_handler.execute_query(sample_query)
    for record in sample_results:
        logger.info(
            f"Sample IS_ABOUT relationship: Statement {record['statement_id']} -> Section {record['section_id']}")


def load_data(neo4j_handler: Neo4jHandler, json_data: Dict[str, Any]):
    try:
        meeting_id = create_meeting(neo4j_handler, json_data)

        if 'people_list' in json_data:
            load_attendance(neo4j_handler, json_data['people_list'], meeting_id)

        if 'sections' in json_data:
            for section in json_data['sections']:
                section_id = create_section(neo4j_handler, section, meeting_id)
                if 'people' in section:
                    create_started_by_relationships(neo4j_handler, section_id, section['people'])
                if 'started_by' in section:
                    create_started_by_relationship(neo4j_handler, section_id, section['started_by'])

        if 'statements' in json_data:
            # Log some debug information about topics
            for stmt in json_data['statements']:  # Look at first 5 statements
                if 'topic' in stmt:
                    logger.info(f"Statement {stmt['id']} has topics: {stmt['topic']}")

            load_statements(neo4j_handler, json_data['statements'], meeting_id)
            # Verify relationships were created
            verify_is_about_relationships(neo4j_handler)

        if 'votes' in json_data:
            load_votes(neo4j_handler, json_data['votes'], meeting_id)

        logger.info(f"Successfully loaded data for meeting: {json_data['name']}")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise




def load_votes(neo4j_handler: Neo4jHandler, votes_data: Dict[str, Any], meeting_id: str):
    for vote in votes_data['votes']:
        vote_id = vote['id']
        # Create Vote node
        query = """
        CREATE (v:Vote {id: $id})
        """
        neo4j_handler.execute_query(query, {'id': vote_id})

        # Create a relationship from Vote to Meeting
        query = """
        MATCH (v:Vote {id: $id})
        MATCH (m:Meeting {id: $meeting_id})
        MERGE (v)-[:MADE_DURING]->(m)
        """
        neo4j_handler.execute_query(query, {
            'id': vote_id,
            'meeting_id': meeting_id
        })

        for vote_type in ['yes', 'no', 'abstained']:
            for person in vote[vote_type]:
                person_name = person['name']
                relationship = vote_type.upper()
                link_person_to_vote_query = f"""
                MATCH (v:Vote {{id: $vote_id}})
                MATCH (p:Person {{name: $person_name}})
                MERGE (p)-[:{relationship}]->(v)
                """
                neo4j_handler.execute_query(link_person_to_vote_query, {
                    'vote_id': vote_id,
                    'person_name': person_name
                })


def link_person_to_party(neo4j_handler: Neo4jHandler, person_name: str, party_name: str):
    query = """
    MERGE (p:Person {name: $person_name})
    MERGE (party:PoliticalParty {name: $party_name})
    MERGE (p)-[:PART_OF]->(party)
    """
    neo4j_handler.execute_query(query, {
        'person_name': person_name,
        'party_name': party_name
    })
DUTCH_MONTHS = {
    'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
    'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
    'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
}

def parse_date_string(date_str: str) -> str:
    try:
        day_str, month_str, year_str = date_str.split()  # e.g. ["23", "augustus", "2024"]
        day = int(day_str)
        month = DUTCH_MONTHS[month_str.lower()]
        year = int(year_str)
        return f"{year:04d}-{month}-{day:02d}"  # e.g. "2024-08-23"
    except (ValueError, KeyError):
        return date_str

def load_data(neo4j_handler: Neo4jHandler, json_data: Dict[str, Any]):
    try:
        meeting_id = create_meeting(neo4j_handler, json_data)

        if 'people_list' in json_data:
            load_attendance(neo4j_handler, json_data['people_list'], meeting_id)

        if 'sections' in json_data:
            for section in json_data['sections']:
                section_id = create_section(neo4j_handler, section, meeting_id)
                if 'people' in section:
                    create_started_by_relationships(neo4j_handler, section_id, section['people'])
                if 'started_by' in section:
                    create_started_by_relationship(neo4j_handler, section_id, section['started_by'])

        if 'statements' in json_data:
            load_statements(neo4j_handler, json_data['statements'], meeting_id)

        if 'votes' in json_data:
            load_votes(neo4j_handler, json_data['votes'], meeting_id)
            # Link persons to their political parties if provided
            if 'political_parties' in json_data:
                for vote in json_data['votes']:
                    for vote_type in ['yes', 'no', 'abstained']:
                        for person in vote[vote_type]:
                            if 'party' in person:
                                link_person_to_party(neo4j_handler, person['name'], person['party'])

        logger.info(f"Successfully loaded data for meeting: {json_data['name']}")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


def load_json_folder(folder_path: str, neo4j_handler: Neo4jHandler):
    for filename in os.listdir(folder_path):
        print(filename)
        if filename.endswith('.json'):
            json_file_path = os.path.join(folder_path, filename)
            with open(json_file_path, 'r') as json_file:
                json_data = json.load(json_file)
                load_data(neo4j_handler, json_data)
                logger.info(f"Loaded data from {filename}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    dag_id='Insert_new_entries_in_database',  # DAG B
    default_args=default_args,
    schedule_interval=None,  # no schedule; it's triggered by DAG A
    catchup=False
) as dag_b:

    def process_triggered_data(**context):
        """
        1) Reads the 'conf' dictionary passed from DAG A (via TriggerDagRunOperator),
           which may include metadata about created files/links/youtubes.
        2) Scans the local directory (/usr/local/airflow/data/json) for JSON files.
        3) Connects to Neo4j and loads the JSON data into the database.
        """

        # Retrieve conf (optional)
        conf = context['dag_run'].conf if context['dag_run'] else {}
        created_files = conf.get('created_files', [])
        links = conf.get('links', [])
        youtubes = conf.get('youtubes', [])

        print("Files from DAG A:", created_files)
        print("Links from DAG A:", links)
        print("YouTube IDs from DAG A:", youtubes)

        # 2) Find JSON files in the local directory
        base_dir = '/usr/local/airflow/data/json'
        json_files = glob.glob(os.path.join(base_dir, '*.json'))
        print(f"Found json files on disk: {json_files}")

        if not json_files:
            print("No .json files found to process.")
            return

        # 3) Connect to Neo4j and load each JSON file
        #    Make sure NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD
        #    are set in your environment or .env
        neo4j_uri = os.getenv("MY_NEO4J_URI", "bolt://neo4j:7687")
        neo4j_user = os.getenv("MY_NEO4J_USER", "neo4j")
        neo4j_password = os.getenv("MY_NEO4J_PASSWORD", "password")

        # Create the Neo4j handler
        handler = Neo4jHandler(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

        # Use the helper function to process *all* .json files in the folder
        load_json_folder(folder_path=base_dir, neo4j_handler=handler)
        handler.close()

        print("Finished processing and loading JSON files into Neo4j.")

    task_process_triggered_data = PythonOperator(
        task_id='process_triggered_data',
        python_callable=process_triggered_data,
        provide_context=True
    )
