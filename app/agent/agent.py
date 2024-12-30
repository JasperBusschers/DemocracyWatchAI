import json
from collections import defaultdict
from typing import Any, List, Dict

from agent.openai.openai_tools import generate_response,get_embedding
from database.data_access_layer import DataAccessLayer, Neo4jHandler
from config.settings import Config
from datetime import date, datetime
from neo4j.time import Date as Neo4jDate, Time as Neo4jTime, DateTime as Neo4jDateTime
from typing import Any


# Initialize configuration and data access layer
conf = Config()
dal = DataAccessLayer(Neo4jHandler(conf.NEO4J_URI, conf.NEO4J_USER, conf.NEO4J_PASSWORD))

# Function and schema descriptions
get_statements_desc = """
Function: get_statements
Retrieves statements from Neo4j based on a question and optional filters (persons, parties, dates, IDs).
Supports sorting by similarity, preference, and recency, and can include adjacent statements.
Only use this function if the question clearly specifies a question about what someone or multiple people said.

Parameters:
- filters (dict, default={}):
  - persons, parties, meetings, statement_ids ,  user_intend, personalized
The user intend is a text based query determined by looking at the question and conversation history and reformulate the question.
Make user_intend by reformulating the question keep it in the same language as the question.
if user asked personalized response, use user profile for refining the user_intend
Example:
{"args": {"filters" : {
    "persons": ["John Doe", "Jane Smith"],
    "parties": ["Groen", "N-VA"],
    "meetings": ["Annual Meeting 2023"],
    "user_intend" : "...",
    "personalized" : 1, (or 0)
}}}

"""


def determine_tool(history, question, preferences):
    prompt = f"""   you are only able to output json.Use execute_query unless the question is about what was said
                    You are given a query and history and your task is to determine what tool should be used and which parameters.
                    output json and nothing else.
                    You can use the helper function when you want to search statements, else you can formulate a query yourself.
                    fun1 get_statements : {get_statements_desc}
                    
                  
                    
                    Use execute_query unless the question is about what was said
                    Output only in json with keys function_name and args

                    """
    user= f"""
  
                    user profile {preferences['subjects']}

                    statements : {history}
                    question : {question}
"""
    response = generate_response(system=prompt,user=user)
    print(response)
    if "``json'" in response:
        response=response.split('``json')[1].split('``')[0]
    return json.loads(response)




def group_statements_by_debate_and_topic(statements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Groups statements by debate (meeting) and then by topic (section_topic),
    structuring each meeting's data under 'meeting_name' and 'content' keys.

    :param statements: A list of statement dictionaries.
    :return: A list of dictionaries, each containing 'meeting_name' and 'content'.
    """
    # Initialize a defaultdict to group by meeting
    grouped_meetings = defaultdict(lambda: defaultdict(list))

    for stmt in statements:
        # Extract meeting name and topic
        meeting_name = stmt.get('meeting', 'Unknown Meeting').strip() +' - '+stmt.get('meetingDate', '').strip()
        topic = stmt.get('section_topic', 'Unknown Topic').strip()

        # Optionally, remove leading colon and whitespace from section_topic
        if topic.startswith(':'):
            topic = topic[1:].strip()

        # Append the statement to the appropriate group
        grouped_meetings[meeting_name]["topic"]=topic
        grouped_meetings[meeting_name]['content'].append(stmt)

    # Transform the grouped data into the desired list of dictionaries
    grouped_list = []
    for meeting, topics in grouped_meetings.items():
        meeting_dict = {
            'meeting_name': meeting,
            'content': {}
        }
        for topic, stmts in topics.items():
            meeting_dict['content'][topic] = stmts
        grouped_list.append(meeting_dict)

    return grouped_list


def reply(history, question, preferences, lang):
    tool = determine_tool(history, question, preferences)
    args =tool['args']
    new_args={}
    vec_weight=1
    tool['function_name'] = 'get_statements'
    if 'filters' in args:
        for a in args['filters']:
            if args['filters'][a]==[] or args['filters'][a]==None or args['filters'][a]=='null' or args['filters'][a]=='':
                pass
            else:
                new_args[a]=args['filters'][a]

        if 'user_intend' in new_args:
            question=new_args['user_intend']
        #if 'personalized' in new_args:
       #     vec_weight=1-new_args['personalized']
    if tool['function_name'] == 'get_statements':
        new_args['vector'] = get_embedding(question)
        new_args['preferences'] = get_embedding(str(preferences['subjects']))
    meeting_ids=dal.get_unique_meeting_ids({'vector':new_args['vector'],'preferences':new_args['preferences']}, preference_weight=preferences['personalization']/100)
    #new_args['meeting_ids']=meeting_ids
    print(meeting_ids)
    print(new_args)
    section_ids = dal.get_relevant_sections(**{
            'meeting_ids': meeting_ids,
            'vector': new_args['vector'],
            'preferences': new_args['preferences']}, preference_weight=preferences['personalization']/100)
    print(section_ids)
    #new_args['section_ids'] = [s['id']for s  in section_ids]
    # Retrieve statements

    results_orig = dal.get_statements(filters=new_args,similarity_weight=vec_weight, preference_weight=preferences['personalization']/100) if tool['function_name'] == 'get_statements' else dal.run_query(
        tool['args']['query'])
    for i in range(len(results_orig)):
        results_orig[i]['reference_number']=i+1
    results = group_statements_by_debate_and_topic(results_orig)
    print(results)
    print(history)
    if tool['function_name'] == 'get_statements':
        prompt = f"""

                        You are given a query and related statements made by politicians. Provide an answer using the statements or if no answer can be found, say you do not know. 
                        Structure your answer per debate and person to summarize what was said about the topic. You may look at the previous and next statement to understand the context of the statement.
                        answer in markdown with bold person names text and unordered lists when applicable, don't take over complete statements but describe what each person said. 
                        Only use the statements that are relevant to the question. Write between brackets next to the person the political party.
                        
                       Only give the statements that are related to the question or user profile, look at the previous and next statement to determine if they are related.
                        Do not give statements that are not related to either the question or profile, unless you are sure the asker is interested in the subject.
                        Do not repeat what was already said in history
                       Think in steps and output only step 4:
                       1) Keep only the statements related to the question
                       2) Remove the statements that occurred in the history already
                       3) Group statements per section and give 2 sentences before next step to set the context.
                       (eg 'In the discussion started by ( look at section_topic ) about ... ')
                       4) Formulate your answer by using the remaining statements. 
                       At the end of each sentence about statements put reference_number to state the sentence refers to statement i. When multiple apply use eg. [1] [2] [5]. 
                        [0] does not exist, it starts from 1. Group statements per meeting, and seperate sections for each relevant meeting. Describe the meeting in 1 line including the date of the meeting and order from recent to oldest..
                        Do not give the complete statements, instead summarize it to provide an answer, do not mention statements that are unrelated to the question.
                        DO NOT REPEAT WHAT IS ALREADY MENTIONED SOMEWHERE IN HISTORY, DO NOT REPEAT YOURSELF
                        reference_number should match the statement that is summarized.
                        All text should be in {lang} except names of people and parties (translate topic).
output format:
**meeting_name**
### Section: [Short description of the topic translated in {lang}]
- **[Name] ([Party])**: [Short summary of the statement.] [reference_number]
- **[Name] ([Party])**: [Short summary of the statement.] [reference_number]
### Section: [Next Topic]
- **[Name] ([Party])**: [Short summary of the statement.] [reference_number]
**[meeting_name]**
### Section: [Short description of the topic]
- **[Name] ([Party])**: [Short summary of the statement.] [reference_number]
- **[Name] ([Party])**: [Short summary of the statement.] [reference_number]

                        """


    else:
        prompt = f"""
                          
                                statements : {results}
                                question : {question}
                                reply in language : {lang}
                                subjects user is generally interested in : {preferences['subjects']}
                                """

    user =f"""
        question : {question}
                        statements : {results}
                        reply in language : {lang}
                        history: {history}
                        user profile {preferences['subjects']}"""
    response = generate_response(user=user, system=prompt)

    return response, [convert_dates_to_strings(r) for r in results_orig]


def convert_dates_to_strings(obj: Any) -> Any:
    """
    Recursively traverse the input object and convert all date and datetime objects to strings.

    Args:
        obj (Any): The input data structure (dict, list, etc.) to process.

    Returns:
        Any: The processed data structure with dates converted to strings.
    """
    if isinstance(obj, dict):
        return {key: convert_dates_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates_to_strings(element) for element in obj]
    elif isinstance(obj, (date, datetime,Neo4jDate)):
        return obj.isoformat()
    else:
        return obj
