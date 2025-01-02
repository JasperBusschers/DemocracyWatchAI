# app.py
from flask import Flask
from flask_jwt_extended import JWTManager
from config.settings import Config
from auth.oauth import init_oauth
from auth.routes import auth_bp
from database.data_access_layer import DataAccessLayer, Neo4jHandler
from core.routes import core_bp
from agent.routes import  agent_bp


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    conf = Config()
    # Initialize extensions
    jwt = JWTManager(app)
    oauth, google = init_oauth(app)
    uri = conf.NEO4J_URI
    user = conf.NEO4J_USER
    password = conf.NEO4J_PASSWORD
    neo4j_handler = Neo4jHandler(uri, user, password)
    dal = DataAccessLayer(neo4j_handler)


    # Register blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(core_bp)
    app.register_blueprint(agent_bp)
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0',debug=Config.DEBUG, port=Config.PORT)