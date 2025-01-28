from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("process_data").enableHiveSupport().getOrCreate()



print("hello")