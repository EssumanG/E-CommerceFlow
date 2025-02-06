import psycopg2
from contextlib import contextmanager

class Connection():
    def __init__(self, user, host, password, database, port, **kwargs):
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            **kwargs
        )

        self.connection = connection

    def get_connection(self):
        return self.connection
    
    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    @contextmanager
    def get_cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally: 
            self.commit()
            cursor.close()
