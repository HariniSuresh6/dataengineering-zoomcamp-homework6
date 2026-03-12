# dataengineering-zoomcamp-homework6
Module 6 Homework

Question 1: Install Spark and PySpark
# Command: 
spark.version
# Answer: 
4.1.1

Question 2: Yellow November 2025
# Python Code: 
df.repartition(4).write.parquet("yellow_output")
# Command: 
ls -lh yellow_output
# Closest answer: 
25MB

Question 3: Count records
# Python Code: 
df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
# Answer:
162,604

# Question 4: Longest trip
# Python Code: 
(unix_timestamp("tpep_dropoff_datetime") -
 unix_timestamp("tpep_pickup_datetime")) / 3600
# Answer:
90.6

Question 5: Spark User Interface
# command
pyspark (will show the port along with the spark shell)
# Answer:
4040

Question 6: Least frequent pickup location zone
# SQL:
SELECT z.Zone, COUNT(*)
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY COUNT(*) ASC
LIMIT 1

# Answer:
Governor's Island/Ellis Island/Liberty Island