#Question 1
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TaxiHomework") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)

#Question 2
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_repartitioned = df.repartition(4)

df_repartitioned.write.parquet("yellow_output")

