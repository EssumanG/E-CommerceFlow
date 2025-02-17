from contextlib import contextmanager
from utils.model import Connection
from spark_config import POSTGRES_PASSWORD, POSTGRES_USER, PORT_DOCKER, POSTGRES_DOCKER_HOST, POSTGRES_DB


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


def create_customer_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id VARCHAR(255) PRIMARY KEY, 
        customer_first_name VARCHAR(100),
        customer_last_name VARCHAR(100),
        customer_segment VARCHAR(200),
        customer_city VARCHAR(100),
        customer_state VARCHAR(100),
        customer_country VARCHAR(100),
        customer_region VARCHAR(100)  
        );
    """)

def create_category_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Categories(
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(200)
        );
    """)

def create_product_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products(
        product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        product_name TEXT,
        category_id INT,
        FOREIGN KEY (category_id) REFERENCES Categories(category_id) 
        );
    """)

def create_orders_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Orders(
        order_id VARCHAR(200) PRIMARY KEY,
        customer_id VARCHAR(200), 
        order_date DATE,
        delivery_status VARCHAR(100),
        shipping_type VARCHAR(100),
        ship_date DATE,
        days_for_shipment_scheduled INT,
        days_for_shipment_real INT,
        sales_per_order INT,  
        profit_per_order DOUBLE PRECISION,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
        );
    """)

def create_orders_items_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Order_Items(
        order_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        order_id VARCHAR(200),
        product_id UUID REFERENCES products(product_id),
        order_item_discount INT,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
        );
    """)



def main():
    #connect to db
    pass
    #create table

if "__main__" == __name__:
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--database", required=True, type=str)
    # parser.add_argument("--user", required=True, type=str )
    # parser.add_argument("--password", required=True, type=str )
    # parser.add_argument("--port", required=True, type=int )
    # parser.add_argument("--host", required=True, type=str)
    # args = parser.parse_args()
      
    try:
        # connection = Connection(
        #         user=args.user,
        #         host=args.host,
        #         password=args.password,
        #         port=args.port,
        #         database=args.database
        # )

        print(f"{POSTGRES_USER}")
        connection = Connection(
                user=str(POSTGRES_USER),
                host=str(POSTGRES_DOCKER_HOST),
                password=str(POSTGRES_PASSWORD),
                port=5432,
                database=str(POSTGRES_DB)
        )


        # connection = Connection(
        #         user="test_user",
        #         host="postgres",
        #         password="test1234",
        #         port=5432,
        #         database="ecommerce"
        # )

        with connection.get_cursor() as cursor:
                
                ##################################################################################################################
                # Trying to normalize the table
                ##########################################################################################################################
                print("----------------Creating Customer Table---------------------------------------")
                create_customer_table(cursor)
                
                print("----------------Creating Orders Table---------------------------------------")
                create_orders_table(cursor)

                print("----------------Creating Categories Table---------------------------------------")
                create_category_table(cursor)

                print("----------------Creating Products Table---------------------------------------")
                create_product_table(cursor)

                print("----------------Creating Order_Items Table---------------------------------------")
                create_orders_items_table(cursor)
                #################################################################################################################################
                connection.commit()
        print("----------------Done Creating Table---------------------------------------")
    except Exception as e:
         print(f"An error occurred: {e}")
    