import requests
from flask import Blueprint, redirect, url_for, request, jsonify, session, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, create_access_token, create_refresh_token, \
    set_access_cookies, set_refresh_cookies, unset_jwt_cookies
from database.data_access_layer import DataAccessLayer, Neo4jHandler
import json
import urllib.parse

from flask_oauthlib.client import OAuth

from config.settings import Config

auth_bp = Blueprint('auth', __name__)

conf=Config()
uri = conf.NEO4J_URI
user = conf.NEO4J_USER
password = conf.NEO4J_PASSWORD
neo4j_handler = Neo4jHandler(uri, user, password)
dal = DataAccessLayer(neo4j_handler)
oauth = OAuth(auth_bp)

facebook = oauth.remote_app(
    'facebook',
    consumer_key='your_facebook_app_id',
    consumer_secret='your_facebook_app_secret',
    request_token_params={'scope': 'email'},
    base_url='https://graph.facebook.com/',
    request_token_url=None,
    access_token_url='/oauth/access_token',
    access_token_method='GET',
    authorize_url='https://www.facebook.com/dialog/oauth'
)



google = oauth.remote_app(
    'google',
    consumer_key=conf.OAUTH_CREDENTIALS['google']['id'],
    consumer_secret=conf.OAUTH_CREDENTIALS['google']['secret'],
    request_token_params={
        'scope': 'email'
    },
    base_url='https://www.googleapis.com/oauth2/v1/',
    request_token_url=None,
    access_token_method='POST',
    access_token_url='https://accounts.google.com/o/oauth2/token',
    authorize_url='https://accounts.google.com/o/oauth2/auth',
)

microsoft = oauth.remote_app(
    'microsoft',
    consumer_key='test',
    consumer_secret='test',
    request_token_params={'scope': 'User.Read'},
    base_url='https://graph.microsoft.com/v1.0/',
    request_token_url=None,
    access_token_method='POST',
    access_token_url='https://login.microsoftonline.com/common/oauth2/v2.0/token',
    authorize_url='https://login.microsoftonline.com/common/oauth2/v2.0/authorize'
)
@auth_bp.route('/login/<provider>')
def oauth_login(provider):
    if provider == 'facebook':
        return facebook.authorize(callback=url_for('auth.oauth_authorized', provider='facebook', _external=True))
    elif provider == 'google':
        return google.authorize(callback=url_for('auth.google_authorized', _external=True))
    elif provider == 'microsoft':
        return microsoft.authorize(callback=url_for('auth.oauth_authorized', provider='microsoft', _external=True))
    else:
        return jsonify({"error": "Unsupported provider"}), 400

@auth_bp.route('/login/authorized/<provider>')
def oauth_authorized(provider):
    if provider == 'facebook':
        resp = facebook.authorized_response()
    elif provider == 'microsoft':
        resp = microsoft.authorized_response()
    else:
        return jsonify({"error": "Unsupported provider"}), 400

    if resp is None or resp.get('access_token') is None:
        error_message = 'Access denied: reason={} error={}'.format(
            request.args['error_reason'],
            request.args['error_description']
        )
        return jsonify({"error": error_message}), 401

    if provider == 'facebook':
        me = facebook.get('/me?fields=email,name')
    elif provider == 'microsoft':
        me = microsoft.get('me')

    user_email = me.data.get('email')
    user_name = me.data.get('name')
    provider_id = me.data.get('id')

    user = dal.create_or_update_user(user_email, user_name, provider, provider_id)

    access_token = create_access_token(identity=user['id'])
    refresh_token = create_refresh_token(identity=user['id'])

    user_info = json.dumps({
        'id': user['id'],
        'email': user['email'],
        'name': user['name']
    })

    return redirect(url_for('core.index',
                            access_token=access_token,
                            refresh_token=refresh_token,
                            user_info=urllib.parse.quote(user_info)))


@auth_bp.route('/login/authorized/google')
def google_authorized():
    try:
        resp = google.authorized_response()
        if resp is None:
            error_message = 'Access denied: reason={} error={}'.format(
                request.args.get('error_reason', 'Unknown'),
                request.args.get('error_description', 'Unknown')
            )
            return jsonify({"error": error_message}), 401

        # Store the OAuth token for subsequent Google API requests
        session['oauth_token'] = (resp['access_token'], '')

        # Get user info using the access token
        headers = {'Authorization': f"Bearer {resp['access_token']}"}
        google_user_info = requests.get('https://www.googleapis.com/oauth2/v3/userinfo', headers=headers).json()

        # Extract user information
        user_email = google_user_info.get('email')
        user_name = google_user_info.get('name')
        provider_id = google_user_info.get('sub')  # Google's unique user identifier

        if not user_email:
            return jsonify({"error": "Failed to get user email from Google"}), 401

        # Create or update user in database
        user = dal.create_or_update_user(user_email, user_name, 'google', provider_id)

        # Create our application's JWT tokens
        access_token = create_access_token(identity=user['id'])
        refresh_token = create_refresh_token(identity=user['id'])

        # Create user info for frontend
        user_info = json.dumps({
            'id': user['id'],
            'email': user['email'],
            'name': user['name']
        })

        # Create response with redirect
        response = redirect(url_for('core.index', user_info=urllib.parse.quote(user_info)))

        # Set secure cookies with our JWT tokens
        set_access_cookies(response, access_token)
        set_refresh_cookies(response, refresh_token)

        return response

    except Exception as e:
        return jsonify({"error": e.__str__()}), 401

@auth_bp.route('/logout', methods=['POST'])
@jwt_required()
def logout():
    try:
        # Create response
        response = make_response(jsonify({"message": "Successfully logged out"}))

        # Unset JWT cookies
        unset_jwt_cookies(response)

        # Additional security headers
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'

        return response, 200

    except Exception as e:
        return jsonify({"error": "Logout failed"}), 500

@auth_bp.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    try:
        current_user = get_jwt_identity()
        new_access_token = create_access_token(identity=current_user)
        return jsonify(access_token=new_access_token), 200
    except Exception as e:
        return jsonify({"error": "Failed to refresh token"}), 500