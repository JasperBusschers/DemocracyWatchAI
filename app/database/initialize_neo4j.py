from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "Jasper"
password = "paddo123"

driver = GraphDatabase.driver(uri, auth=(username, password))

driver = GraphDatabase.driver(uri, auth=(username, password))

def delete_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def add_country(tx, country_name):
    tx.run("CREATE (c:Country {name: $name})", name=country_name)

def add_political_party(tx, country_name, party_name):
    tx.run("""
    MATCH (c:Country {name: $country_name})
    CREATE (p:PoliticalParty {name: $party_name})-[:IN_COUNTRY]->(c)
    """, country_name=country_name, party_name=party_name)

def add_representative(tx, party_name, rep_name):
    tx.run("""
    MATCH (p:PoliticalParty {name: $party_name})
    CREATE (r:Representative {name: $rep_name})-[:MEMBER_OF]->(p)
    """, party_name=party_name, rep_name=rep_name)

def add_debate(tx, debate_name):
    tx.run("CREATE (d:Debate {name: $name})", name=debate_name)

def add_topic(tx, debate_name, topic_name):
    tx.run("""
    MATCH (d:Debate {name: $debate_name})
    CREATE (t:Topic {name: $topic_name})-[:DISCUSSED_IN]->(d)
    """, debate_name=debate_name, topic_name=topic_name)

def add_statement(tx, rep_name, topic_name, content):
    tx.run("""
    MATCH (r:Representative {name: $rep_name}), (t:Topic {name: $topic_name})
    CREATE (s:Statement {content: $content})-[:MADE_BY]->(r)-[:ON]->(t)
    """, rep_name=rep_name, topic_name=topic_name, content=content)

def add_vote(tx, rep_name, topic_name, vote_type):
    tx.run("""
    MATCH (r:Representative {name: $rep_name}), (t:Topic {name: $topic_name})
    CREATE (v:Vote {type: $vote_type})-[:CAST_BY]->(r)-[:ON]->(t)
    """, rep_name=rep_name, topic_name=topic_name, vote_type=vote_type)

with driver.session() as session:
    session.write_transaction(delete_all)  # Clearing all existing data
    session.write_transaction(add_country, "SampleLand")
    session.write_transaction(add_political_party, "SampleLand", "Green Party")
    session.write_transaction(add_representative, "Green Party", "Alice Johnson")
    session.write_transaction(add_debate, "Debate on Climate Change")
    session.write_transaction(add_topic, "Debate on Climate Change", "Global Warming")
    session.write_transaction(add_statement, "Alice Johnson", "Global Warming", "We need to act now!")
    session.write_transaction(add_vote, "Alice Johnson", "Global Warming", "Yes")
    session.write_transaction(delete_all)  # Clearing all existing data

driver.close()