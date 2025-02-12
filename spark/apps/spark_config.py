import os

#spark
DATA_SOURCE_URL = os.environ.get("DATA_SOURCE_URL")
CSV_FILE_DIR = os.environ.get("CSV_FILE_DIR")
JDBC_DB_URL = os.environ.get("JDBC_DB_URL")


POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DOCKER_HOST = os.environ.get("POSTGRES_DOCKER_HOST")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
PORT_DOCKER = os.environ.get("PORT_DOCKER")
PORT_HOST = os.environ.get("PORT_HOST")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
