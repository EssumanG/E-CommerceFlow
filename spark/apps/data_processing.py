from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import functions as F
import uuid

spark = SparkSession.builder.appName("process_data")\
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
df = spark.read.option("header",True).option("encoding", "UTF-8").schema(schema).csv("/opt/spark/resources/data/ecommerce/Ecommerce_data_100.csv")
# df = spark.read.option("header",True).schema(schema).csv("/opt/spark_app/data/ecommerce/Ecommerce_data.csv")

df = df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
df = df.withColumn("ship_date", F.to_date("ship_date", "yyyy-MM-dd"))

# Define a UDF to generate UUID
uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

#Extracting unique customers info
customer_df = df.select(
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_segment",
    "customer_city",
    "customer_state",
    "customer_country",
    "customer_region"
).dropDuplicates(["customer_id"])
customer_df.printSchema()


#Extracting unique category info and assigning uuid to the df
catergory_df = df.select(
    "category_name"
).distinct().withColumn("category_id", uuid_udf())
catergory_df.printSchema()


# Join category_id with product data
product_df = df.select("product_name", "category_name").dropDuplicates(["product_name"])
product_df = product_df.join(
    catergory_df, "category_name", "left"
).select("product_name", "category_id")

# assigning uuid to the df
product_df = product_df.withColumn("product_id", uuid_udf())
product_df.printSchema()


#Extracting unique orders data
order_df = df.select(
    "order_id",
    "customer_id",
    "order_date",
    "delivery_status",
    "shipping_type",
    "ship_date",
    "days_for_shipment_scheduled",
    "days_for_shipment_real",
    "sales_per_order",
    "profit_per_order"
).dropDuplicates(["order_id"])
order_df.printSchema()



print("The number of rows-------------------------")
catergory_df.show(2)

# df_10 = df_new.limit(10)


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