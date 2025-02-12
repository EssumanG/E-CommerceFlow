from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
from spark.apps.spark_config import JDBC_DB_URL, POSTGRES_DB, POSTGRES_PASSWORD, POSTGRES_USER, PORT_DOCKER


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





products_analysis = df.select("product_name", "category_name", "order_id", "sales_per_order", "profit_per_order", "order_quantity", "customer_segment")\
    .groupBy("product_name")\
    .agg(
        F.count("order_id").alias("total_orders"),
        F.try_divide(F.sum(F.col("sales_per_order")), F.sum(F.col("order_quantity"))).alias("unit_cost"),
        F.sum("sales_per_order").alias("total_sales"),
        F.avg("profit_per_order").alias("avg_profit"),
        F.sum("order_quantity").alias("total_quantity_sold")
    ).orderBy(F.desc("total_sales"))
products_analysis.show(10)


top_customers = df.groupBy("customer_id", "customer_first_name", "customer_last_name")\
    .agg(
        F.sum("sales_per_order").alias("total_revenue"),
        F.count("order_id").alias("total_orders")
    )
top_customers.show(10)

customer_segments = df.groupBy("customer_segment") \
    .agg(
        F.count("customer_id").alias("total_customers"),
        F.sum("sales_per_order").alias("total_revenue"),
        F.avg("profit_per_order").alias("avg_profit")
    ) \
    .orderBy(F.desc("total_revenue"))

top_customers.show(10)


geo_demand = df.groupBy("customer_city", "customer_state", "customer_region") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("sales_per_order").alias("total_revenue")
    ) \
    .orderBy(F.desc("total_orders"))

geo_demand.show(10)

df_dates = df.withColumn("year", F.year("order_date"))\
    .withColumn("month", F.month("order_date"))

seasonal_sales = df_dates.groupBy("year", "month")\
    .agg(
        F.sum("sales_per_order").alias("total_revenue"),
        F.avg("sales_per_order").alias("avg_sales"),
        F.avg("profit_per_order").alias("avg_profit"),
        F.sum("profit_per_order").alias("total_profit"),
        F.count("order_id").alias("total_orders")
    ).orderBy("year", "month")
seasonal_sales.show(10)

try:
    
    products_analysis.write.format("jdbc") \
        .option("url", JDBC_DB_URL) \
        .option("dbtable", "public.product_analysis") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    
    top_customers.write.format("jdbc") \
        .option("url", JDBC_DB_URL) \
        .option("dbtable", "public.top_customers") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    
    geo_demand.write.format("jdbc") \
        .option("url", JDBC_DB_URL) \
        .option("dbtable", "public.geo_demand") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    
    seasonal_sales.write.format("jdbc") \
        .option("url", JDBC_DB_URL) \
        .option("dbtable", "public.seasonal_sales") \
        .option("user", POSTGRES_USER) \
        .option("password",  POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    customer_segments.write.format("jdbc") \
        .option("url", JDBC_DB_URL) \
        .option("dbtable", "public.customer_segment_analysis") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
except Exception as e:
    print(f"ERORR---------{e}")

print("hello")