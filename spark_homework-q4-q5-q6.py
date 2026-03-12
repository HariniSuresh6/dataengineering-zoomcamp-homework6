from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp

# Create Spark Session
spark = SparkSession.builder \
    .appName("DEZoomcampSparkHomework") \
    .getOrCreate()

# Load dataset (this was missing)
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")


print("\n========== QUESTION 4 ==========")

df_trip = df.withColumn(
    "trip_hours",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 3600
)

longest_trip = df_trip.selectExpr("max(trip_hours)").collect()[0][0]

print("Longest trip duration (hours):", longest_trip)


print("\n========== QUESTION 5 ==========")

print("Spark UI runs on port: 4040")


print("\n========== QUESTION 6 ==========")

# Load taxi zone lookup
zones = spark.read.option("header", True).csv("taxi_zone_lookup.csv")

# Create temporary views
df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

least_zone = spark.sql("""
SELECT z.Zone, COUNT(*) as trip_count
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trip_count ASC
LIMIT 1
""")

least_zone.show()

spark.stop()