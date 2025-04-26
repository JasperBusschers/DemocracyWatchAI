# dag_b_process.py
"""
DAG B: Processes the files downloaded by DAG A. This DAG is triggered by DAG A
using TriggerDagRunOperator, and it also demonstrates how to read from local files
on the same Airflow server, then load them into Neo4j.

Now updated to:
- Use caching for embeddings on /usr/local/airflow/data/vectors
- Use os.getenv('OPENAI_KEY') for the API key
"""

import os
import glob
import json
import logging
import uuid
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests  # for OpenAI API calls

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dotenv import load_dotenv
from neo4j import GraphDatabase

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Embedding + Cache
# -------------------------------------------------------------------------
def get_embedding(text: str, api_key: str) -> List[float]:
    """
    Calls the OpenAI Embeddings API to get an embedding for the given text,
    but first checks a cache dir (/usr/local/airflow/data/vectors).
    If an embedding for the same text is found, returns it from cache.
    Otherwise, calls the API, stores it, and returns it.
    """

    # Make sure the cache directory exists
    cache_dir = "/usr/local/airflow/data/vectors"
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    # Hash the text to make a stable filename
    text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
    cache_file = os.path.join(cache_dir, f"{text_hash}.json")

    # If cache file exists, load and return
    if os.path.isfile(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_vector = json.load(f)
            logging.info(f"Loaded embedding from cache for hash={text_hash}")
            return cached_vector
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Failed to load cached embedding {cache_file}: {e}")

    # Otherwise, call the API
    url = "https://api.openai.com/v1/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    data = {
        "input": text,
        "model": "text-embedding-3-small"
    }

    logging.info(f"Requesting embedding from OpenAI for text_hash={text_hash} len={len(text)}")
    resp = requests.post(url, headers=headers, json=data)
    if resp.status_code == 200:
        vector = resp.json()['data'][0]['embedding']
        # Save it to cache
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(vector, f)
            logging.info(f"Saved embedding to cache: {cache_file}")
        except OSError as e:
            logging.warning(f"Failed to write cache file {cache_file}: {e}")

        return vector
    else:
        raise Exception(f"OpenAI embedding error {resp.status_code}: {resp.text}")

# -------------------------------------------------------------------------
# Dutch months to numeric
# -------------------------------------------------------------------------
DUTCH_MONTHS = {
    'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
    'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
    'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
}

def parse_date_string(date_str: str) -> str:
    """Convert a Dutch date string (e.g. '9 oktober 2024') to ISO-8601 '2024-10-09'."""
    try:
        day_str, month_str, year_str = date_str.split()
        day = int(day_str)
        month = DUTCH_MONTHS[month_str.lower()]
        year = int(year_str)
        return f"{year:04d}-{month}-{day:02d}"
    except (ValueError, KeyError):
        return date_str

# -------------------------------------------------------------------------
# Neo4j Handler
# -------------------------------------------------------------------------
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

# -------------------------------------------------------------------------
# Create Meeting
# -------------------------------------------------------------------------
def create_meeting(neo4j_handler: Neo4jHandler, json_data: Dict[str, Any]) -> str:
    meeting_id = str(uuid.uuid4())
    iso_date = parse_date_string(json_data.get('date', '01 januari 1970'))
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
        'name': json_data.get('name'),
        'date': iso_date,
        'country': json_data.get('country'),
        'region': json_data.get('region'),
        'youtube_link': 'https://www.youtube.com/watch?v=' + json_data.get('youtube_link', ''),
        'pdf_link': json_data.get('pdf_link', '')
    })
    return meeting_id

# -------------------------------------------------------------------------
# Sections (embedded)
# -------------------------------------------------------------------------
def create_section(neo4j_handler: Neo4jHandler, section: Dict[str, Any], meeting_id: str, api_key: str) -> str:
    section_id = section.get('id') or str(uuid.uuid4())
    # combine type + topic as embedding text
    raw_text = (section.get('type','') + ' ' + section.get('topic','')).strip()
    if not raw_text:
        raw_text = "No Section Text"

    embedding = get_embedding(raw_text, api_key)

    query = """
    CREATE (s:Section {
        id: $id,
        type: $type,
        voted: $voted,
        vector: $vector,
        topic: $topic
    })
    WITH s
    MATCH (m:Meeting {id: $meeting_id})
    CREATE (m)-[:HAS_SECTION]->(s)
    """
    neo4j_handler.execute_query(query, {
        'id': section_id,
        'type': section.get('type'),
        'voted': section.get('voted', False),
        'vector': embedding,
        'topic': section.get('topic',''),
        'meeting_id': meeting_id
    })
    return section_id

def create_started_by_relationships(neo4j_handler: Neo4jHandler, section_id: str, people: List[str]):
    for person_name in people:
        query = """
        MATCH (s:Section {id: $section_id})
        MERGE (p:Person {name: $person_name})
        MERGE (p)-[:IS_STARTED_BY]->(s)
        """
        neo4j_handler.execute_query(query, {
            'section_id': section_id,
            'person_name': person_name
        })

def create_started_by_relationship(neo4j_handler: Neo4jHandler, section_id: str, person_name: str):
    query = """
    MATCH (s:Section {id: $section_id})
    MERGE (p:Person {name: $person_name})
    MERGE (p)-[:STARTED]->(s)
    """
    neo4j_handler.execute_query(query, {
        'section_id': section_id,
        'person_name': person_name
    })

# -------------------------------------------------------------------------
# Attendance
# -------------------------------------------------------------------------
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

# -------------------------------------------------------------------------
# parse_markdown_summarized, create Topics + Articles with embeddings
# -------------------------------------------------------------------------
def parse_markdown_summarized(ms: str) -> Dict[str, Any]:
    try:
        return json.loads(ms)
    except (json.JSONDecodeError, TypeError):
        return {}

def create_topics_from_topic_texts(neo4j_handler: Neo4jHandler, topic_texts: List[Dict[str, Any]], api_key: str):
    filtered = [
        t for t in topic_texts
        if 'markdown_summarized' in t and t['markdown_summarized']
    ]
    if not filtered:
        return

    topics_to_create = []
    articles_to_create = []

    for t in filtered:
        parsed = parse_markdown_summarized(t['markdown_summarized'])
        description = parsed.get('summary', '')
        text_for_topic_embedding = description if description else "No Topic Description"
        topic_vector = get_embedding(text_for_topic_embedding, api_key)

        topics_to_create.append({
            'title': t['title'],
            'date': t.get('date'),
            'link': t.get('link'),
            'filepath': t.get('filepath'),
            'meeting_id': t.get('meeting_id'),
            'description': description,
            'vector': topic_vector
        })

        # articles
        if 'articles' in parsed and isinstance(parsed['articles'], list):
            for article in parsed['articles']:
                raw_txt = (article.get('text','') + ' ' + article.get('4sentence_description','')).strip()
                if not raw_txt:
                    raw_txt = "No Article Text"
                article_vec = get_embedding(raw_txt, api_key)

                articles_to_create.append({
                    'topic_title': t['title'],
                    'number': article.get('number'),
                    'text': article.get('text'),
                    'four_sentence_desc': article.get('4sentence_description', ''),
                    'vector': article_vec
                })

    # Create Topics
    create_topics_query = """
    UNWIND $topics AS t
    MERGE (topic:Topic {title: t.title})
    SET topic.date = t.date,
        topic.link = t.link,
        topic.filepath = t.filepath,
        topic.meeting_id = t.meeting_id,
        topic.description = t.description,
        topic.vector = t.vector
    """
    neo4j_handler.execute_query(create_topics_query, {'topics': topics_to_create})

    # Create Articles
    if articles_to_create:
        create_articles_query = """
        UNWIND $articles AS art
        MATCH (topic:Topic {title: art.topic_title})
        MERGE (a:Article {
            number: art.number,
            text: art.text,
            four_sentence_desc: art.four_sentence_desc
        })
        SET a.vector = art.vector
        MERGE (topic)-[:HAS_ARTICLE]->(a)
        """
        neo4j_handler.execute_query(create_articles_query, {'articles': articles_to_create})

# -------------------------------------------------------------------------
# Link Section -> Topic
# -------------------------------------------------------------------------
def link_section_to_topic_for_voted(neo4j_handler: Neo4jHandler, section_id: str, vote_text: str):
    query = """
    MATCH (s:Section {id: $section_id})
    MATCH (t:Topic {title: $vote_text})
    MERGE (s)-[:HAS_TOPIC]->(t)
    SET s.topic = t.description
    """
    neo4j_handler.execute_query(query, {
        'section_id': section_id,
        'vote_text': vote_text
    })

# -------------------------------------------------------------------------
# Statements (embedded)
# -------------------------------------------------------------------------
def load_statements(neo4j_handler: Neo4jHandler, statements: List[Dict[str, Any]], meeting_id: str, api_key: str):
    if not statements:
        return

    statements_data = []
    for stmt in statements:
        statement_text = stmt.get('statement','') or "No Statement Text"
        emb = get_embedding(statement_text, api_key)

        statements_data.append({
            'id': stmt['id'],
            'statement': stmt.get('statement',''),
            'Party': stmt.get('Party'),
            'person': stmt.get('person'),
            'person_ref': stmt.get('person_ref'),
            'topic': stmt.get('topic'),
            'previous': stmt.get('previous'),
            'next': stmt.get('next'),
            'vector': emb
        })

    # Create statements
    create_stmt_query = """
    UNWIND $stmts AS st
    MERGE (s:Statement {id: st.id})
    SET s.statement = st.statement,
        s.vector = st.vector,
        s.party = st.Party,
        s.person = st.person
    """
    neo4j_handler.execute_query(create_stmt_query, {'stmts': statements_data})

    # Person -> Statement
    create_made_relationships_query = """
    UNWIND $stmts AS st
    MATCH (s:Statement {id: st.id})
    MATCH (p:Person {name: st.person_ref})
    MERGE (p)-[:MADE]->(s)
    """
    neo4j_handler.execute_query(create_made_relationships_query, {'stmts': statements_data})

    # Link statement <-> sections
    statements_with_topics = []
    for st in statements_data:
        if st['topic'] and isinstance(st['topic'], list):
            statements_with_topics.append({
                'statement_id': st['id'],
                'section_ids': st['topic']
            })

    if statements_with_topics:
        create_is_about_query = """
        UNWIND $swts AS st
        UNWIND st.section_ids AS sid
        MATCH (s:Statement {id: st.statement_id})
        MATCH (sec:Section {id: sid})
        MERGE (s)-[:IS_ABOUT]->(sec)
        MERGE (sec)-[:HAS_STATEMENT]->(s)
        """
        neo4j_handler.execute_query(create_is_about_query, {'swts': statements_with_topics})

    # Statement -> Meeting
    create_made_during_query = """
    UNWIND $stmts AS st
    MATCH (s:Statement {id: st.id})
    MATCH (m:Meeting {id: $meeting_id})
    MERGE (s)-[:MADE_DURING]->(m)
    """
    neo4j_handler.execute_query(create_made_during_query, {
        'stmts': statements_data,
        'meeting_id': meeting_id
    })

    # Next/Previous
    create_next_query = """
    UNWIND $stmts AS st
    MATCH (s:Statement {id: st.id})
    WHERE st.next IS NOT NULL
    MATCH (nxt:Statement {id: st.next})
    MERGE (s)-[:HAS_NEXT]->(nxt)
    """
    neo4j_handler.execute_query(create_next_query, {'stmts': statements_data})

    create_prev_query = """
    UNWIND $stmts AS st
    MATCH (s:Statement {id: st.id})
    WHERE st.previous IS NOT NULL
    MATCH (prv:Statement {id: st.previous})
    MERGE (s)-[:HAS_PREVIOUS]->(prv)
    """
    neo4j_handler.execute_query(create_prev_query, {'stmts': statements_data})

    logger.info(f"Loaded {len(statements_data)} statements with embeddings.")

# -------------------------------------------------------------------------
# Votes
# -------------------------------------------------------------------------
def load_votes(neo4j_handler: Neo4jHandler, votes_data: Dict[str, Any], meeting_id: str):
    if not votes_data or 'votes' not in votes_data:
        return

    for vote in votes_data['votes']:
        vote_id = vote['id']
        # Create Vote node
        query = """
        CREATE (v:Vote {id: $id})
        """
        neo4j_handler.execute_query(query, {'id': vote_id})

        # Link (Vote)-[:MADE_DURING]->(Meeting)
        query = """
        MATCH (v:Vote {id: $id})
        MATCH (m:Meeting {id: $meeting_id})
        MERGE (v)-[:MADE_DURING]->(m)
        """
        neo4j_handler.execute_query(query, {
            'id': vote_id,
            'meeting_id': meeting_id
        })

        # People -> Vote
        for vote_type in ['yes', 'no', 'abstained']:
            for person_info in vote.get(vote_type, []):
                person_name = person_info['name']
                relationship = vote_type.upper()
                link_person_to_vote_query = f"""
                MATCH (v:Vote {{id: $vote_id}})
                MERGE (p:Person {{name: $person_name}})
                MERGE (p)-[:{relationship}]->(v)
                """
                neo4j_handler.execute_query(link_person_to_vote_query, {
                    'vote_id': vote_id,
                    'person_name': person_name
                })

# -------------------------------------------------------------------------
# Main loader
# -------------------------------------------------------------------------

def load_data(neo4j_handler: Neo4jHandler, json_data: Dict[str, Any], api_key: str):
    """
    Main entry point for loading a single JSON file's data into Neo4j,
    with embeddings cached on disk in /usr/local/airflow/data/vectors
    """
    try:
        name =json_data.get('name')
        if True:
            # 1) Meeting
            meeting_id = create_meeting(neo4j_handler, json_data)

            # 2) Attendance
            if 'people_list' in json_data:
                load_attendance(neo4j_handler, json_data['people_list'], meeting_id)

            # 3) Topics & Articles
            topic_texts = json_data.get('topic_texts', [])
            create_topics_from_topic_texts(neo4j_handler, topic_texts, api_key)

            # 4) Sections
            if 'sections' in json_data:
                for section in json_data['sections']:
                    section_id = create_section(neo4j_handler, section, meeting_id, api_key)
                    if 'people' in section:
                        create_started_by_relationships(neo4j_handler, section_id, section['people'])
                    if 'started_by' in section:
                        create_started_by_relationship(neo4j_handler, section_id, section['started_by'])
                    if section.get('voted') and 'vote_text' in section and section['vote_text']:
                        link_section_to_topic_for_voted(neo4j_handler, section_id, section['vote_text'])

            # 5) Statements
            if 'statements' in json_data:
                load_statements(neo4j_handler, json_data['statements'], meeting_id, api_key)

            # 6) Votes
            if 'votes' in json_data:
                load_votes(neo4j_handler, json_data['votes'], meeting_id)

            logger.info(f"Successfully loaded data for meeting: {json_data.get('name')}")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def load_json_folder(folder_path: str, neo4j_handler: Neo4jHandler, api_key: str):
    """
    Scans a folder for .json files and calls load_data(...) on each.
    """
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            json_file_path = os.path.join(folder_path, filename)
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)
                load_data(neo4j_handler, data, api_key)
                logger.info(f"Loaded data from {filename}")

# -------------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------------
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
        1) Reads the 'conf' dictionary passed from DAG A (via TriggerDagRunOperator).
        2) Scans the local directory (/usr/local/airflow/data/json) for JSON files.
        3) Connects to Neo4j and loads the JSON data into the database, using embeddings with caching.
        """
        conf = context['dag_run'].conf if context['dag_run'] else {}
        created_files = conf.get('created_files', [])
        links = conf.get('links', [])
        youtubes = conf.get('youtubes', [])

        print("Files from DAG A:", created_files)
        print("Links from DAG A:", links)
        print("YouTube IDs from DAG A:", youtubes)

        base_dir = '/usr/local/airflow/data/json'
        json_files = glob.glob(os.path.join(base_dir, '*.json'))
        print(f"Found JSON files on disk: {json_files}")

        if not json_files:
            print("No .json files found to process.")
            return

        # Retrieve the OpenAI API key from environment
        openai_api_key = os.getenv("OPENAI_KEY", "")
        if not openai_api_key:
            raise ValueError("No OPENAI_KEY found in environment. Please set it before running the DAG.")

        # Neo4j credentials
        neo4j_uri = os.getenv("MY_NEO4J_URI", "bolt://neo4j:7687")
        neo4j_user = os.getenv("MY_NEO4J_USER", "neo4j")
        neo4j_password = os.getenv("MY_NEO4J_PASSWORD", "password")

        # Create the Neo4j handler
        handler = Neo4jHandler(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

        # Load all JSON files in the folder
        load_json_folder(folder_path=base_dir, neo4j_handler=handler, api_key=openai_api_key)

        handler.close()
        print("Finished processing and loading JSON files into Neo4j.")

    task_process_triggered_data = PythonOperator(
        task_id='process_triggered_data',
        python_callable=process_triggered_data,
        provide_context=True
    )
