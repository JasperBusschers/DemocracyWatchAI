import traceback
from typing import Optional, Dict, Any

from flask import Blueprint, render_template, request, jsonify
from agent.openai.openai_tools import get_embedding, generate_response
from database.data_access_layer import DataAccessLayer, Neo4jHandler
from neo4j import GraphDatabase
from flask_jwt_extended import jwt_required, get_jwt_identity, verify_jwt_in_request, create_access_token

from config.settings import Config
from neo4j.exceptions import Neo4jError

from agent.agent2 import reply

# Initialize Blueprint and Database Connection
agent_bp = Blueprint('agent', __name__)
conf=Config()
uri = conf.NEO4J_URI
user = conf.NEO4J_USER
password = conf.NEO4J_PASSWORD
neo4j_handler = Neo4jHandler(uri, user, password)
dal = DataAccessLayer(neo4j_handler)
def get_current_user() -> Optional[Dict[str, Any]]:
    """
    Get the current user if authenticated.
    Returns None if not authenticated or error occurs.
    """
    try:
        verify_jwt_in_request(optional=True)
        current_user_id = get_jwt_identity()
        if current_user_id:
            user = dal.get_user_by_id(current_user_id)
            return user
    except Exception as e:
        raise e
    return None
@agent_bp.route('/partials/statement_sidebar')
def sidebar_partial():
    """Render the sidebar template."""
    return render_template('partials/statement_sidebar.html')

@agent_bp.route('/statement', methods=['GET'])
def get_statement():
    """Retrieve a statement and its metadata, including likes/dislikes."""
    statement_id = request.args.get('statement_id')
    direction = request.args.get('direction')  # 'next' or 'previous'
    user_id = request.args.get('user_id')  # User ID to track reactions

    try:
        current_user = get_current_user()
        if direction == 'next':
            statement = dal.get_next(statement_id)
        elif direction == 'previous':
            statement = dal.get_previous(statement_id)
        else:
            statement = dal.get_current(statement_id)

        if statement:
            statement_data = statement[0]
            statement_id = statement_data['statement_id']
            reactions = dal.get_statement_likes_dislikes(statement_id)
            statement_data.update(reactions)
            statement_data['user_reaction'] = dal.get_user_reaction(user_id, statement_id) if user_id else None

            return jsonify(statement_data), 200

        return jsonify({"error": "Statement not found"}), 404
    except Exception as e:
        print(f"Error fetching statement: {str(e)}")
        return jsonify({"error": str(e)}), 500
@agent_bp.route('/ask', methods=['POST'])
@jwt_required(optional=True)
def ask():
    """Answer user questions using AI-generated responses based on statements."""
    data = request.json or {}
    question = data.get('question')
    lang = data.get('language', 'en')
    conversation_id = data.get('conversation_id')  # The conversation to add messages to

    try:
        current_user = get_current_user()
        user_id = current_user['id'] if current_user else None
        # --- New daily message limit check ---
        if user_id:
            daily_count = dal.get_user_daily_message_count(user_id)
            if daily_count >= conf.MAX_DAILY_MESSAGES:
                return jsonify({
                    'response': "Error : You have exceeded your daily limit of 10 questions.",
                    'statements': [],
                    'conversation_id': "",
                    'conversation_title': ""  # Return the newly generated title if any
                }), 200
        # --- GENERATE A CONVERSATION TITLE IF WE'RE STARTING FRESH ---
        new_conversation_title = None
        past_interactions = []
        if user_id and not conversation_id:
            # Prompt the AI for a short descriptive conversation title
            title_prompt_system = f"""
            Please propose a short conversation title (max 6 words) based on this question:
            
            The title should hint at the topic or subject.
            reply in language : {lang}
            """
            user_prompt= f"""{question}"""
            new_conversation_title = generate_response(system=title_prompt_system,user=user_prompt,max_tokens=50).strip()

            # If it’s too long or empty, fallback to "New Conversation"
            if not new_conversation_title or len(new_conversation_title) > 60:
                new_conversation_title = "New Conversation"

            # Create the conversation
            conversation_id = dal.create_conversation(user_id, title=new_conversation_title)
        elif conversation_id and user_id:
            # Retrieve past messages
            messages = dal.get_chat_messages_with_citations(conversation_id)
            for msg in messages:
                # Validate conversation ownership
                conversation = dal.get_conversation(conversation_id, user_id)
                if not conversation:
                    return jsonify({"error": "Invalid conversation_id or not owned by user"}), 403
                statements=  msg.get('statements', [])
                if len(statements)>0:
                    print(statements)
                    statements= dal.get_statements({"statement_ids":statements})
                # Retrieve related statements

                # Retrieve related debates/meetings
                past_debates = []
                for statement in statements:
                    meeting = statement['meeting']
                    if meeting:
                        past_debates.append(meeting)

                past_interactions.append({
                    'past_answer': msg.get('answer'),
                    'past_question': msg.get('question'),
                    'past_statements': [s['statement_id'] for s in statements],
                    'past_debates': past_debates
                })
        preferences = dal.get_user_preferences(user_id)
        preferences = {'subjects': preferences['interests'], 'personalization': preferences['personalization_weight']}
        print(past_interactions)
        print(preferences)
        # --- (Optional) validate conversation ownership if user_id + conversation_id ---
        if user_id and conversation_id:
            conversation = dal.get_conversation(conversation_id, user_id)
            if not conversation:
                return jsonify({"error": "Invalid conversation_id or not owned by user"}), 403
        response,statements= reply(past_interactions,question,preferences,lang)

        # Retrieve statements relevant to the question
        #statements = dal.get_statements({'vector': get_embedding(question),'preferences':get_embedding(str(preferences['subjects']))},include_prev_next=True,preference_weight=preferences['personalization']/100)
        statement_ids = []
        for row in statements:
            if 'statement_id' in row and row['statement_id']:
                # row['statement_id'] might be e.g. ['ac51f37c-979c-4f2e-aa4f-c0d0a5eabcb4', '06f987e0-d735-47c6-a14a-c0e018f41cc7']
                statement_ids.extend(row['statement_id'])
        print(statement_ids)

        # Build your main prompt


        # Save chat message if user is logged in + conversation exists
        if user_id and conversation_id:
            chat_message_id=dal.save_chat_message(conversation_id, question, response)
            print(chat_message_id,statement_ids)
            if chat_message_id and statement_ids:
                dal.link_message_to_citations(chat_message_id, statement_ids)
        return jsonify({
            'response': response,
            'statements': statements,
            'conversation_id': conversation_id,
            'conversation_title': new_conversation_title  # Return the newly generated title if any
        }), 200

    except Exception as e:
        print(f"Error answering question: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@agent_bp.route('/meetings', methods=['GET'])
def get_meeting():
    """Retrieve meeting details for a given statement."""
    statement_id = request.args.get('statement_id')
    if not statement_id:
        return jsonify({"error": "Statement ID is required"}), 400

    query = """
    MATCH (s:Statement {id: $statement_id})-[:MADE_DURING]->(m:Meeting)
    RETURN m.name AS name,
           m.date AS date,
           m.country AS country,
           m.region AS region,
           m.pdf_link AS pdf_link,
           m.youtube_link AS youtube_link,
           m.id AS id
    LIMIT 1
    """

    try:
        result = neo4j_handler.execute_query(query, {'statement_id': statement_id})
        for row in result:
            if 'date' in row and row['date']:
                row['date'] = str(row['date'])
        if result and result[0]:
            return jsonify(result[0]), 200
        return jsonify({"error": "Meeting not found"}), 404
    except Exception as e:
        print(f"Error getting meeting info: {str(e)}")
        return jsonify({"error": str(e)}), 500
@jwt_required()
@agent_bp.route('/like_statement', methods=['POST'])
def like_statement():
    """Like a statement."""
    data = request.json
    verify_jwt_in_request()
    user_id  = get_jwt_identity()
    statement_id = data.get('statement_id')
    print(user_id,statement_id)
    try:
        current_user = get_current_user()
        dal.like_statement(user_id, statement_id)
        return jsonify({"message": "Statement liked successfully"}), 200
    except Exception as e:
        print(f"Error liking statement: {str(e)}")
        return jsonify({"error": str(e)}), 500
@jwt_required()
@agent_bp.route('/dislike_statement', methods=['POST'])
def dislike_statement():
    """Dislike a statement."""
    data = request.json
    verify_jwt_in_request()
    user_id = get_jwt_identity()
    statement_id = data.get('statement_id')

    try:
        current_user = get_current_user()
        dal.dislike_statement(user_id, statement_id)
        return jsonify({"message": "Statement disliked successfully"}), 200
    except Exception as e:
        print(f"Error disliking statement: {str(e)}")
        return jsonify({"error": str(e)}), 500

@agent_bp.route('/get_reaction', methods=['GET'])
def get_reaction():
    statement_id = request.args.get('statement_id')
    verify_jwt_in_request()
    user_id = get_jwt_identity()
    if not statement_id or not user_id:
        return jsonify({"error": "Missing statement_id or user_id"}), 400

    try:
        current_user = get_current_user()
        reaction = dal.get_user_reaction(user_id, statement_id)
        return jsonify({"reaction": reaction}), 200
    except Exception as e:
        print(f"Error fetching reaction: {str(e)}")
        return jsonify({"error": str(e)}), 500
@agent_bp.route('/conversation/<conversation_id>', methods=['GET'])
@jwt_required()
def get_conversation_messages(conversation_id):
    user_id = get_jwt_identity()
    conversation = dal.get_conversation(conversation_id, user_id)
    if not conversation:
        return jsonify({"error": "No such conversation or unauthorized"}), 404

    messages = dal.get_chat_messages_with_citations(conversation_id)
    print(messages)
    for m in messages:
        filters = {
            "statement_ids": m['statements'],
            # Add other filters if necessary, e.g., "persons": ["John Doe"]
        }

        # Retrieve statements by IDs
        m['statements'] = dal.get_statements(filters)
    conversation["messages"] = messages
    return jsonify(conversation), 200


# In data_access_layer.py (or whichever file your DAL is in)

@agent_bp.route('/chat_history', methods=['GET'])
@jwt_required()
def get_chat_history():
    """
    Returns all chat messages for the current user across all conversations.
    """
    try:
        current_user = get_current_user()
        user_id = get_jwt_identity()
        # Pull messages from the DAL
        messages = dal.get_all_chat_messages_for_user(user_id)

        return jsonify({"messages": messages}), 200

    except Exception as e:
        print(f"Error fetching chat history: {str(e)}")
        return jsonify({"error": str(e)}), 500

@agent_bp.route('/search_preferences', methods=['GET'])
@jwt_required()
def get_search_preferences():
    """
    Retrieve the current user's personalization settings.
    """
    try:
        user_id = get_jwt_identity()
        if not user_id:
            return jsonify({"error": "User not authenticated"}), 401

        preferences = dal.get_user_preferences(user_id)

        return jsonify({'subjects':preferences['interests'], 'personalization': preferences['personalization_weight']}), 200

    except Neo4jError as ne:
        print(f"Neo4j Error: {ne.message}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        return jsonify({"error": "An unexpected error occurred"}), 500


@agent_bp.route('/save_search_preferences', methods=['POST'])
@jwt_required()
def save_search_preferences():
    """
    Save or update the current user's personalization settings.
    """
    try:
        user_id = get_jwt_identity()

        preferences = dal.get_user_preferences(user_id)

        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        interests = data.get('subjects')

        personalization_weight = int(data.get('personalization'))

        # Prepare the preferences dictionary
        if interests is not None:
            preferences['interests'] = interests
        if personalization_weight is not None:
            preferences['personalization_weight'] = personalization_weight
        print(preferences)
        if not preferences:
            return jsonify({"error": "No valid settings provided"}), 400

        # Update the user's preferences in the database
        updated_preferences = dal.update_user_preferences(user_id, preferences)

        return jsonify({
            "message": "Search preferences updated successfully",
            "settings": updated_preferences
        }), 200

    except Neo4jError as ne:
        # Specific handling for Neo4j errors
        print(f"Neo4j Error: {ne.message}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        # General exception handling
        print(f"Unexpected Error: {str(e)}")
        return jsonify({"error": "An unexpected error occurred"}), 500