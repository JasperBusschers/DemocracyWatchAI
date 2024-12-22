from typing import Optional, Dict, Any

from flask import Blueprint, render_template, request, jsonify
from agent.openai.openai_tools import get_embedding, generate_response
from database.data_access_layer import DataAccessLayer, Neo4jHandler
from neo4j import GraphDatabase
from flask_jwt_extended import jwt_required, get_jwt_identity, verify_jwt_in_request, create_access_token

from config.settings import Config

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
        if user_id and not conversation_id:
            # Prompt the AI for a short descriptive conversation title
            title_prompt = f"""
            Please propose a short conversation title (max 6 words) based on this question:
            '{question}'
            The title should hint at the topic or subject.
            """
            new_conversation_title = generate_response(title_prompt,max_tokens=50).strip()

            # If it’s too long or empty, fallback to "New Conversation"
            if not new_conversation_title or len(new_conversation_title) > 60:
                new_conversation_title = "New Conversation"

            # Create the conversation
            conversation_id = dal.create_conversation(user_id, title=new_conversation_title)

        # --- (Optional) validate conversation ownership if user_id + conversation_id ---
        if user_id and conversation_id:
            conversation = dal.get_conversation(conversation_id, user_id)
            if not conversation:
                return jsonify({"error": "Invalid conversation_id or not owned by user"}), 403

        # Retrieve statements relevant to the question
        statements = dal.get_statements({'vector': get_embedding(question)})
        print(statements)

        # Build your main prompt
        prompt = f"""
                You are given a query and related statements made by politicians. Provide an answer using the statements or if no answer can be found, say you do not know. 
                Structure your answer per debate and person to summarize what was said about the topic.
                answer in markdown with bold person names text and unordered lists when applicable, don't take over complete statements but describe what each person said. 
                Only use the statements that are relevant to the question.
                At the end of each sentence put [i+1] (eg [1] for index 0) to state the sentence refers to statement i. When multiple apply use eg. [1] [2] [5]. 
                [0] does not exist, it starts from 1.
                Do not give the complete statements, instead summarize it to provide an answer.
                statements : {statements}
                question : {question}
                reply in language : {lang}
                """

        response = generate_response(prompt)

        # Save chat message if user is logged in + conversation exists
        if user_id and conversation_id:
            dal.save_chat_message(conversation_id, question, response)

        return jsonify({
            'response': response,
            'statements': statements,
            'conversation_id': conversation_id,
            'conversation_title': new_conversation_title  # Return the newly generated title if any
        }), 200

    except Exception as e:
        print(f"Error answering question: {str(e)}")
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
    """Return all messages in a single conversation if user owns it."""
    user_id = get_jwt_identity()
    # Validate that the user owns that conversation
    conversation = dal.get_conversation(conversation_id, user_id)
    if not conversation:
        return jsonify({"error": "No such conversation or unauthorized"}), 404

    messages = dal.get_chat_messages_by_conversation(conversation_id)
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
