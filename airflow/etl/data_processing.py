from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import functions as F
import psycopg2
import psycopg2.extras as pg_extras

spark = SparkSession.builder.appName("process_data")\
        .config("spark.jars", "/opt/spark_app/jars/postgresql-42.5.0.jar")\
            .getOrCreate()


schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("customer_country", StringType(), True),
    StructField("customer_region", StringType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("ship_date", StringType(), True),
    StructField("shipping_type", StringType(), True),
    StructField("days_for_shipment_scheduled", IntegerType(), True),
    StructField("days_for_shipment_real", IntegerType(), True),
    StructField("order_item_discount", DoubleType(), True),
    StructField("sales_per_order", DoubleType(), True),
    StructField("order_quantity", IntegerType(), True),
    StructField("profit_per_order", DoubleType(), True)
])
df = spark.read.option("header",True).option("encoding", "UTF-8").schema(schema).csv("/opt/airflow/datasets/ecommerce/Ecommerce_data.csv")
# df = spark.read.option("header",True).schema(schema).csv("/opt/spark_app/data/ecommerce/Ecommerce_data.csv")

df = df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
df = df.withColumn("ship_date", F.to_date("ship_date", "yyyy-MM-dd"))
df = df.withColumn("customer_name", F.concat_ws(",","customer_first_name","customer_last_name"))
df_new = df.drop("profit_per_order", "customer_first_name", "customer_last_name" )

df_new.printSchema()


df_new.show(10,truncate=False)


# TODO: Fix Error: get stuck at when writing to postgres db or othe file
# df_new.coalesce(4).write.option("header", True).mode("overwrite").csv("./output/ecommerce_transformed.csv")

# db_url = "jdbc:postgresql://postgres:5432/airflow"
# db_properties = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}
# df_new.write.format("jdbc") \
#     .option("url", db_url) \
#     .option("dbtable", "public.ecommerce") \
#     .option("user", db_properties["user"]) \
#     .option("password", db_properties["password"]) \
#     .option("driver", db_properties["driver"]) \
#     .mode("append") \
#     .save()

print("hello")