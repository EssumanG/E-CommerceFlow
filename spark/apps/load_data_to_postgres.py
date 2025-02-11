from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import functions as F
import uuid

spark = SparkSession.builder.appName("process_data")\
            .config("spark.jars", "/opt/spark/resources/jars/postgresql-42.7.3.jar") \
            .config("spark.driver.extraClassPath", "/opt/spark/resources/jars/postgresql-42.7.3.jar") \
            .config("spark.executor.extraClassPath", "/opt/spark/resources/jars/postgresql-42.7.3.jar") \
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
df = spark.read.option("header",True)\
    .option("encoding", "UTF-8")\
        .schema(schema)\
            .csv("/opt/spark/resources/data/ecommerce/Ecommerce_data.csv")
# df = spark.read.option("header",True).schema(schema).csv("/opt/spark_app/data/ecommerce/Ecommerce_data.csv")

df = df.withColumn("order_date_p1",
    F.coalesce(
        F.to_date(F.col("order_date"), "d/M/yyyy"),  # Handles "11/5/2022"
        F.to_date(F.col("order_date"), "dd-MM-yyyy") # Handles "20-06-2022"
    ))
df = df.drop("order_date").withColumnRenamed("order_date_p1", "order_date")



df = df.withColumn("ship_date_p1",
    F.coalesce(
        F.to_date(F.col("ship_date"), "d/M/yyyy"),  # Handles "11/5/2022"
        F.to_date(F.col("ship_date"), "dd-MM-yyyy") # Handles "20-06-2022"
    ))
df = df.drop("ship_date").withColumnRenamed("ship_date_p1", "ship_date")

df_t = df.select(
    "order_id",
    "product_name",
    "category_name",
    "order_date",
    "ship_date",
    "shipping_type"
)

count = df.count()

print(f"The number of rows-------------------------{count}")






# df_t.show(10)
# df_10 = df_new.limit(10)



db_url = "jdbc:postgresql://postgres:5432/ecommerce"
db_properties = {"user": "test_user", "password": "test1234", "driver": "org.postgresql.Driver"}
# try:
#     df.write.format("jdbc") \
#         .option("url", db_url) \
#         .option("dbtable", "public.ecommerce") \
#         .option("user", "test_user") \
#         .option("password", "test1234") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()
# except Exception as e:
#     print(f"ERORR---------{e}")

print("hello")