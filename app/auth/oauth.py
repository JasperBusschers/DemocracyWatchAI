# auth/oauth.py
from flask_oauthlib.client import OAuth
from config.settings import Config

def init_oauth(app):
    oauth = OAuth(app)

    google = oauth.remote_app(
        'google',
        consumer_key=Config.OAUTH_CREDENTIALS['google']['id'],
        consumer_secret=Config.OAUTH_CREDENTIALS['google']['secret'],
        request_token_params={'scope': 'email'},
        base_url='https://www.googleapis.com/oauth2/v1/',
        request_token_url=None,
        access_token_method='POST',
        access_token_url='https://accounts.google.com/o/oauth2/token',
        authorize_url='https://accounts.google.com/o/oauth2/auth'
    )

    # Add other OAuth providers here...

    return oauth, google