# Data Engineering Zoomcamp – Module 6 Homework

## Question 1: Install Spark and PySpark

**Command**

```python
spark.version
```

**Answer**

```
4.1.1
```

---

## Question 2: Yellow November 2025

**Python Code**

```python
df.repartition(4).write.parquet("yellow_output")
```

**Command**

```bash
ls -lh yellow_output
```

**Closest Answer**

```
25MB
```

---

## Question 3: Count Records

**Python Code**

```python
df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
```

**Answer**

```
162,604
```

---

## Question 4: Longest Trip

**Python Code**

```python
(unix_timestamp("tpep_dropoff_datetime") -
 unix_timestamp("tpep_pickup_datetime")) / 3600
```

**Answer**

```
90.6
```

---

## Question 5: Spark User Interface

**Command**

```bash
pyspark
```

This command starts the Spark shell and displays the Spark UI port.

**Answer**

```
4040
```

---

## Question 6: Least Frequent Pickup Location Zone

**SQL Query**

```sql
SELECT z.Zone, COUNT(*)
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY COUNT(*) ASC
LIMIT 1;
```

**Answer**

```
Governor's Island/Ellis Island/Liberty Island
```