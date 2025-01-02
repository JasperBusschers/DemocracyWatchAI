# config/settings.py
import os
from datetime import timedelta
from dotenv import load_dotenv
class Config:
    load_dotenv('../.env')
    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'your_secret_key')
    DEBUG = os.getenv('DEBUG', False)
    PORT = 5007#int(os.getenv('PORT', 5000))

    # JWT settings
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your_jwt_secret_key')
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=30)
    JWT_TOKEN_LOCATION = ['cookies']
    JWT_COOKIE_CSRF_PROTECT = False
    JWT_COOKIE_SECURE = True
    JWT_COOKIE_SAMESITE = 'Lax'

    # Session settings
    SESSION_TYPE = 'filesystem'
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'

    # Database settings
    NEO4J_URI = os.getenv('MY_NEO4J_URI', 'bolt://localhost:7687')
    NEO4J_USER = os.getenv('MY_NEO4J_USER', 'neo4j')
    NEO4J_PASSWORD = os.getenv('MY_NEO4J_PASSWORD')

    # OAuth settings
    OAUTH_CREDENTIALS = {
        'google': {
            'id': os.getenv('GOOGLE_AUTH_ID'),
            'secret': os.getenv('GOOGLE_AUTH_SECRET')
        },
        'facebook': {
            'id': 'your_facebook_app_id',
            'secret': 'your_facebook_app_secret'
        },
        'microsoft': {
            'id': 'test',
            'secret': 'test'
        }
    }

    OPEN_AI_KEY = os.getenv('OPENAI_KEY')
    MAX_DAILY_MESSAGES = os.getenv('MAX_DAILY_MESSAGES',1000)