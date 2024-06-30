import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from a .env file one level up
env_path = Path('../..') / '.env'
load_dotenv(dotenv_path=env_path)

NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
EMBEDDING_PROVIDER = os.getenv('EMBEDDING_PROVIDER')
HUGGINGFACE_MODEL = os.getenv('HUGGINGFACE_MODEL')
AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT')
AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_KEY')
