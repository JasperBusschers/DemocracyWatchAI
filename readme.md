# Airflow, Neo4j, and Python Application Docker Compose Setup

## Quickstart

Follow these steps to set up the development environment using Docker Compose.

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo.git
cd your-repo
```

2. Create a .env File
Create a .env file in the root directory of the project and add the following environment variables:

env
Copy code
# Neo4j Configuration
MY_NEO4J_URI="bolt://neo4j:7687"
MY_NEO4J_USER=
MY_NEO4J_PASSWORD=
MY_NEO4J_DATABASE=

# OpenAI API Key
OPENAI_KEY=""  # Replace with your actual OpenAI API key

# Google OAuth Credentials
GOOGLE_AUTH_ID=""
GOOGLE_AUTH_SECRET=''

# Airflow Configuration
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_FIRSTNAME=Admin
AIRFLOW_LASTNAME=User
AIRFLOW_EMAIL=admin@example.com
Important:

Replace the MY_NEO4J_PASSWORD and OPENAI_KEY values with your actual credentials.
Do not commit the .env file to version control to protect sensitive information.
# 3. Ensure Prerequisites Are Installed
Make sure you have the following installed on your machine:

# 4. Start the Services
Run the following command to build and start all the services in detached mode:

```bash
docker-compose up -d
```
# 5. Access the Services
Once the containers are up and running, you can access the services via your web browser:

Airflow Webserver: http://localhost:8080
Neo4j Browser: http://localhost:7474
Python Application: http://localhost:5007

# 6. run the data fetching pipelines
Go to airflow webserver and run the following DAGs:
- download_debates_flanders
- Insert_new_entries_in_database

# 7. Stopping the Services
To stop and remove the containers, networks, and volumes defined in the docker-compose.yml, run:

```bash
docker-compose down
```
