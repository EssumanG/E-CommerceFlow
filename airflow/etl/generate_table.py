import psycopg2
from contextlib import contextmanager
import argparse
from model import Connection
# from psycopg2.extensions import connection, cursor

@contextmanager  
def cursor_manager(connection):
    """
    Context manager for managing a database cursor
    """
    cursor = connection.cursor()
    try:
        yield cursor
    finally: 
        cursor.close()


def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ecommerce (
        cutomer_id VARCHAR(200) PRIMARY KEY, 
        customer_first_name VARCHAR(100),
        customer_last_name VARCHAR(100),
        category_name VARCHAR(100),
        product_name VARCHAR(100),
        customer_segment VARCHAR(100),
        customer_city VARCHAR(100),
        customer_state VARCHAR(100),
        customer_country VARCHAR(100),
        customer_region VARCHAR(100),
        delivery_status VARCHAR(100),
        order_date DATE,
        order_id VARCHAR(100),
        ship_date DATE,
        shipping_type VARCHAR(100),
        days_for_shipment_scheduled INT,
        days_for_shipment_real INT,   
        order_item_discount INT,
        sales_per_order INT,
        order_quantity INT       
        );
    """)


def main():
    #connect to db
    pass
    #create table

if "__main__" == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=str)
    parser.add_argument("--user", required=True, type=str )
    parser.add_argument("--password", required=True, type=str )
    parser.add_argument("--port", required=True, type=int )
    parser.add_argument("--host", required=True, type=str)
    args = parser.parse_args()
      
    try:
        connection = Connection(
                user=args.user,
                host=args.host,
                password=args.password,
                port=args.port,
                database=args.database
        )
        with connection.get_cursor() as cursor:
                create_table(cursor)
                connection.commit()
    except Exception as e:
         print(f"An error occurred: {e}")
    