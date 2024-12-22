from flask import Blueprint, render_template, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity, verify_jwt_in_request, create_access_token
from logging import getLogger
from typing import Optional, Dict, Any

from database.data_access_layer import DataAccessLayer, Neo4jHandler
from config.language import LanguageManager
from config.settings import Config


conf=Config()
uri = conf.NEO4J_URI
user = conf.NEO4J_USER
password = conf.NEO4J_PASSWORD
neo4j_handler = Neo4jHandler(uri, user, password)
dal = DataAccessLayer(neo4j_handler)
logger = getLogger(__name__)
core_bp = Blueprint('core', __name__)

language_manager = LanguageManager()

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
            logger.debug(f"Found user: {user}")
            return user
    except Exception as e:
        logger.error(f"Error getting user: {str(e)}")
    return None

@core_bp.route('/')
def index():
    """Main page route with language support and user state."""
    lang = request.args.get('lang', 'en')
    if not language_manager.is_language_supported(lang):
        lang = 'en'
        logger.warning(f"Unsupported language requested: {lang}")

    show_login = request.args.get('show_login', 'false').lower() == 'true'
    user = get_current_user()

    if user:
        logger.debug(f"Rendering template with user: {user}")
    else:
        logger.debug("Rendering template without user")
    print(language_manager.get_translations(lang))
    return render_template(
        'index.html',
        texts=language_manager.get_translations(lang),
        languages=language_manager.get_available_languages(),
        current_lang=lang,
        show_login=show_login,
        user=user
    )
@core_bp.route('/chatbot/<region>')
@jwt_required()
def chatbot(region: str):
    """Chatbot interface route with region-specific content."""
    lang = request.args.get('lang', 'en')
    if not language_manager.is_language_supported(lang):
        lang = 'en'
        logger.warning(f"Unsupported language requested: {lang}")

    current_user = get_jwt_identity()
    user = dal.get_user_by_id(current_user)

    logger.info(f"Loading chatbot for region: {region}, user: {user['email']}")

    return render_template(
        'chat.html',
        texts=language_manager.get_translations(lang),
        languages=language_manager.get_available_languages(),
        current_lang=lang,
        user=user,
        region=region
    )

@core_bp.route('/settings')
@jwt_required()
def settings():
    """User settings route."""
    current_user = get_jwt_identity()
    user = dal.get_user_by_id(current_user)
    logger.debug(f"Fetching settings for user: {user['email']}")
    return jsonify(user), 200


@core_bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors."""
    logger.warning(f"404 error: {request.url}")
    return render_template('errors/404.html'), 404

@core_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    logger.error(f"500 error: {str(error)}")
    return render_template('errors/500.html'), 500

# API health check
@core_bp.route('/health')
def health_check():
    """API health check endpoint."""
    try:
        # Add any necessary health checks here
        # db_status = dal.check_connection()
        return jsonify({
            "status": "healthy",
            "version": "1.0.0"
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500
