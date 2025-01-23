import psycopg2


def create_connection():
    try:
        conn = psycopg2.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="localhost"
        )
        return conn
    except Exception as error:
        print(f"Couldn't create database: {error}")
        return None