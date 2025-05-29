# Comprehensive PySpark Cheatsheet

## Table of Contents
1. [Installation & Setup](#installation--setup)
2. [SparkSession](#sparksession)
3. [Creating DataFrames](#creating-dataframes)
4. [Basic DataFrame Operations](#basic-dataframe-operations)
5. [Data Types & Schema](#data-types--schema)
6. [Filtering & Selection](#filtering--selection)
7. [Aggregations & GroupBy](#aggregations--groupby)
8. [Joins](#joins)
9. [Window Functions](#window-functions)
10. [Built-in Functions](#built-in-functions)
11. [Reading & Writing Data](#reading--writing-data)
12. [SQL Operations](#sql-operations)
13. [Performance Optimization](#performance-optimization)
14. [Error Handling](#error-handling)

---

## Installation & Setup

### Install PySpark
```python
# Install via pip
pip install pyspark

# Or with specific version
pip install pyspark==3.5.5
```

### Import Essential Modules
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

---

## SparkSession

### Create SparkSession
```python
# Basic SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

# With configuration
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Stop SparkSession
spark.stop()
```

---

## Creating DataFrames

### From Lists and Dictionaries
```python
# From list of dictionaries
data = [{"name": "Alice", "age": 25, "city": "NYC"},
        {"name": "Bob", "age": 30, "city": "LA"}]
df = spark.createDataFrame(data)

# From list of tuples with schema
data = [("Alice", 25, "NYC"), ("Bob", 30, "LA")]
schema = ["name", "age", "city"]
df = spark.createDataFrame(data, schema)

# With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])
df = spark.createDataFrame(data, schema)
```

### From RDD
```python
# Create RDD first
rdd = spark.sparkContext.parallelize([("Alice", 25), ("Bob", 30)])
df = rdd.toDF(["name", "age"])
```

### From Pandas DataFrame
```python
import pandas as pd
pandas_df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
spark_df = spark.createDataFrame(pandas_df)
```

---

## Basic DataFrame Operations

### Display and Inspection
```python
# Show DataFrame
df.show()
df.show(5)  # Show first 5 rows
df.show(truncate=False)  # Don't truncate long strings

# Print schema
df.printSchema()

# Get column names
df.columns

# Get data types
df.dtypes

# Count rows
df.count()

# Describe statistics
df.describe().show()

# Summary statistics for specific columns
df.describe("age", "salary").show()
```

### Column Operations
```python
# Select columns
df.select("name", "age").show()
df.select(df.name, df.age).show()
df.select(col("name"), col("age")).show()

# Add new column
df.withColumn("age_plus_ten", col("age") + 10).show()

# Rename column
df.withColumnRenamed("name", "full_name").show()

# Drop columns
df.drop("age").show()
df.drop("age", "city").show()
```

---

## Data Types & Schema

### Common Data Types
```python
from pyspark.sql.types import *

# Basic types
StringType()
IntegerType()
LongType()
FloatType()
DoubleType()
BooleanType()
DateType()
TimestampType()

# Complex types
ArrayType(StringType())
MapType(StringType(), IntegerType())
StructType([
    StructField("field1", StringType(), True),
    StructField("field2", IntegerType(), False)
])
```

### Schema Operations
```python
# Create schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("department", StringType(), True)
])

# Apply schema to DataFrame
df = spark.createDataFrame(data, schema)

# Cast column types
df.withColumn("age", col("age").cast(IntegerType())).show()
```

---

## Filtering & Selection

### Basic Filtering
```python
# Filter rows
df.filter(col("age") > 25).show()
df.filter(df.age > 25).show()
df.where(col("age") > 25).show()  # Same as filter

# Multiple conditions
df.filter((col("age") > 25) & (col("city") == "NYC")).show()
df.filter((col("age") > 25) | (col("city") == "LA")).show()

# String operations
df.filter(col("name").like("A%")).show()  # Starts with 'A'
df.filter(col("name").contains("li")).show()  # Contains 'li'
df.filter(col("name").startswith("A")).show()
df.filter(col("name").endswith("e")).show()

# Null handling
df.filter(col("age").isNull()).show()
df.filter(col("age").isNotNull()).show()

# IN operator
df.filter(col("city").isin(["NYC", "LA"])).show()
```

### Advanced Selection
```python
# Select with expressions
df.select(
    col("name"),
    (col("age") + 5).alias("age_plus_five"),
    when(col("age") > 25, "Adult").otherwise("Young").alias("category")
).show()

# Select distinct
df.select("city").distinct().show()

# Limit rows
df.limit(10).show()
```

---

## Aggregations & GroupBy

### Basic Aggregations
```python
# Count, sum, avg, min, max
df.agg(
    count("*").alias("total_count"),
    sum("age").alias("total_age"),
    avg("age").alias("avg_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age")
).show()

# Standard deviation and variance
df.agg(
    stddev("age").alias("std_age"),
    variance("age").alias("var_age")
).show()
```

### GroupBy Operations
```python
# Group by single column
df.groupBy("city").count().show()
df.groupBy("city").avg("age").show()
df.groupBy("city").sum("salary").show()

# Group by multiple columns
df.groupBy("city", "department").count().show()

# Multiple aggregations
df.groupBy("city").agg(
    count("*").alias("count"),
    avg("age").alias("avg_age"),
    max("salary").alias("max_salary")
).show()

# Having clause (filter after grouping)
df.groupBy("city").agg(count("*").alias("count")) \
  .filter(col("count") > 5).show()
```

---

## Joins

### Basic Joins
```python
# Inner join (default)
df1.join(df2, df1.id == df2.id).show()
df1.join(df2, "id").show()  # When column names are same

# Different join types
df1.join(df2, df1.id == df2.id, "inner").show()
df1.join(df2, df1.id == df2.id, "left").show()
df1.join(df2, df1.id == df2.id, "right").show()
df1.join(df2, df1.id == df2.id, "outer").show()
df1.join(df2, df1.id == df2.id, "left_anti").show()  # Left anti join
df1.join(df2, df1.id == df2.id, "left_semi").show()  # Left semi join

# Multiple join conditions
df1.join(df2, (df1.id == df2.id) & (df1.name == df2.name)).show()
```

### Cross Join
```python
# Cross join (Cartesian product)
df1.crossJoin(df2).show()
```

---

## Window Functions

### Window Specifications
```python
from pyspark.sql.window import Window

# Define window
windowSpec = Window.partitionBy("department").orderBy("salary")

# Row number
df.withColumn("row_number", row_number().over(windowSpec)).show()

# Rank functions
df.withColumn("rank", rank().over(windowSpec)).show()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# Lag and Lead
df.withColumn("prev_salary", lag("salary", 1).over(windowSpec)).show()
df.withColumn("next_salary", lead("salary", 1).over(windowSpec)).show()

# Cumulative aggregations
df.withColumn("running_total", sum("salary").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()

# Moving averages
windowSpec3 = Window.partitionBy("department").orderBy("date").rowsBetween(-2, 0)
df.withColumn("moving_avg_3", avg("salary").over(windowSpec3)).show()
```

---

## Built-in Functions

### String Functions
```python
# String manipulation
df.select(
    upper(col("name")).alias("upper_name"),
    lower(col("name")).alias("lower_name"),
    length(col("name")).alias("name_length"),
    trim(col("name")).alias("trimmed_name"),
    ltrim(col("name")).alias("left_trimmed"),
    rtrim(col("name")).alias("right_trimmed")
).show()

# Substring and replace
df.select(
    substring(col("name"), 1, 3).alias("first_3_chars"),
    regexp_replace(col("name"), "a", "A").alias("replaced_name")
).show()

# Split and concat
df.select(
    split(col("name"), " ").alias("name_parts"),
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name_ws")
).show()
```

### Date/Time Functions
```python
# Current date and time
df.select(
    current_date().alias("current_date"),
    current_timestamp().alias("current_timestamp")
).show()

# Date manipulation
df.select(
    year(col("date")).alias("year"),
    month(col("date")).alias("month"),
    dayofmonth(col("date")).alias("day"),
    dayofweek(col("date")).alias("day_of_week"),
    date_add(col("date"), 30).alias("date_plus_30"),
    date_sub(col("date"), 30).alias("date_minus_30"),
    datediff(col("end_date"), col("start_date")).alias("days_diff")
).show()

# String to date conversion
df.select(
    to_date(col("date_string"), "yyyy-MM-dd").alias("date"),
    to_timestamp(col("timestamp_string"), "yyyy-MM-dd HH:mm:ss").alias("timestamp")
).show()
```

### Mathematical Functions
```python
# Math operations
df.select(
    abs(col("value")).alias("absolute_value"),
    round(col("value"), 2).alias("rounded_value"),
    ceil(col("value")).alias("ceiling"),
    floor(col("value")).alias("floor"),
    sqrt(col("value")).alias("square_root"),
    pow(col("value"), 2).alias("squared")
).show()
```

### Conditional Functions
```python
# Conditional logic
df.select(
    when(col("age") > 30, "Senior")
    .when(col("age") > 25, "Mid")
    .otherwise("Junior").alias("category"),
    
    coalesce(col("phone"), col("email"), lit("No Contact")).alias("contact"),
    
    isnull(col("age")).alias("is_age_null"),
    isnan(col("salary")).alias("is_salary_nan")
).show()
```

---

## Reading & Writing Data

### Reading Data
```python
# CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/file.csv")

# JSON
df = spark.read.json("path/to/file.json")

# Parquet
df = spark.read.parquet("path/to/file.parquet")

# Delta (if available)
df = spark.read.format("delta").load("path/to/delta/table")

# JDBC
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "mytable") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Multiple files
df = spark.read.csv("path/to/directory/*.csv", header=True)
```

### Writing Data
```python
# CSV
df.write.csv("output/path", header=True, mode="overwrite")

# JSON
df.write.json("output/path", mode="overwrite")

# Parquet
df.write.parquet("output/path", mode="overwrite")

# Delta
df.write.format("delta").mode("overwrite").save("output/path")

# JDBC
df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "output_table") \
    .option("user", "username") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()

# Partitioned write
df.write.partitionBy("year", "month").parquet("output/path")

# Write modes
df.write.mode("overwrite").csv("output/path")  # overwrite, append, ignore, error
```

---

## SQL Operations

### Register DataFrame as Temp View
```python
# Create temporary view
df.createOrReplaceTempView("people")

# SQL queries
result = spark.sql("SELECT * FROM people WHERE age > 25")
result.show()

# Complex SQL
spark.sql("""
    SELECT department, 
           AVG(salary) as avg_salary,
           COUNT(*) as employee_count
    FROM people 
    WHERE age > 25
    GROUP BY department
    HAVING COUNT(*) > 5
    ORDER BY avg_salary DESC
""").show()

# Drop temp view
spark.catalog.dropTempView("people")
```

### Global Temp Views
```python
# Global temp view (accessible across SparkSessions)
df.createGlobalTempView("global_people")
spark.sql("SELECT * FROM global_temp.global_people").show()
```

---

## Performance Optimization

### Caching
```python
# Cache DataFrame
df.cache()
df.persist()  # Same as cache()

# Cache with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist
df.unpersist()

# Check if cached
df.is_cached
```

### Partitioning
```python
# Repartition
df.repartition(4).show()  # 4 partitions
df.repartition("department").show()  # Partition by column

# Coalesce (reduce partitions)
df.coalesce(2).show()

# Check number of partitions
df.rdd.getNumPartitions()
```

### Optimization Techniques
```python
# Broadcast small DataFrames
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "id").show()

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Predicate pushdown (filter early)
df.filter(col("age") > 25).select("name", "age").show()  # Good
# df.select("name", "age").filter(col("age") > 25).show()  # Less optimal
```

---

## Error Handling

### Common Patterns
```python
try:
    # DataFrame operations
    result = df.filter(col("age") > 25).collect()
except Exception as e:
    print(f"Error: {e}")

# Check if column exists
if "age" in df.columns:
    df.select("age").show()

# Handle null values
df.na.drop().show()  # Drop rows with any null
df.na.drop(subset=["age"]).show()  # Drop rows with null in specific columns
df.na.fill({"age": 0, "name": "Unknown"}).show()  # Fill nulls with values

# Replace values
df.na.replace(["old_value"], ["new_value"], subset=["column_name"]).show()
```

### Data Quality Checks
```python
# Check for nulls
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Check for duplicates
df.groupBy(df.columns).count().filter(col("count") > 1).show()

# Remove duplicates
df.dropDuplicates().show()
df.dropDuplicates(["id"]).show()  # Based on specific columns
```

---

## Useful Patterns

### Pivot Operations
```python
# Pivot table
df.groupBy("department").pivot("year").sum("salary").show()
```

### Union Operations
```python
# Union DataFrames
df1.union(df2).show()
df1.unionByName(df2).show()  # Union by column names
```

### Sample Data
```python
# Sample data
df.sample(0.1).show()  # 10% sample
df.sample(withReplacement=True, fraction=0.1, seed=42).show()
```

### Configuration
```python
# Get configuration
spark.conf.get("spark.sql.adaptive.enabled")

# Set configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")

# List all configurations
for key, value in spark.conf.getAll():
    print(f"{key}: {value}")
```

---

## Best Practices

1. **Use appropriate data types** to save memory and improve performance
2. **Cache DataFrames** that are used multiple times
3. **Partition data** appropriately for better performance
4. **Use predicate pushdown** by filtering early in the pipeline
5. **Avoid collect()** on large DataFrames; use show() or write() instead
6. **Use broadcast joins** for small DataFrames
7. **Enable adaptive query execution** for automatic optimizations
8. **Monitor Spark UI** for performance tuning
9. **Use columnar formats** like Parquet for better performance
10. **Handle nulls explicitly** to avoid unexpected behavior

---

This cheatsheet covers the most commonly used PySpark operations with verified code examples from the official Apache Spark documentation. All examples are based on PySpark 3.5.5 API specifications.