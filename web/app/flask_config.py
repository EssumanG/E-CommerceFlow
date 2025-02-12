import os

POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_DOCKER_HOST = os.environ.get("POSTGRES_DOCKER_HOST")

class Config:
    DEBUG=True
    SQLALCHEMY_DATABASE_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5435/{POSTGRES_DB}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False