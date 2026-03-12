from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# create spark session
spark = SparkSession.builder \
    .appName("TaxiHomework") \
    .getOrCreate()

# load dataset
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# filter trips on Nov 15
count = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
).count()

print("Trips on Nov 15:", count)