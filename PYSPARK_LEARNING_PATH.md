# PySpark Learning Path: Beginner to Advanced

**Duration:** 8-12 weeks  
**Prerequisite:** Python basics (loops, functions, data structures)  
**Goal:** Master PySpark for production data pipelines

---

## TABLE OF CONTENTS

- [LEVEL 1: Foundations (Week 1-2)](#level-1-foundations)
- [LEVEL 2: Core Operations (Week 3-4)](#level-2-core-operations)
- [LEVEL 3: Intermediate (Week 5-6)](#level-3-intermediate)
- [LEVEL 4: Advanced (Week 7-8)](#level-4-advanced)
- [LEVEL 5: Production Patterns (Week 9-10)](#level-5-production-patterns)

---

# LEVEL 1: FOUNDATIONS (Week 1-2)

## Topic 1.1: What is Spark?

### Concept

Spark is a **distributed computing framework** that processes large datasets across multiple computers in parallel.

### Why Spark?

```
Single Machine (Traditional):
  Data: 1GB
  Processing: Sequential
  Time: 10 minutes

Spark Cluster:
  Data: 1TB (1000x larger)
  Processing: Parallel (100 computers)
  Time: 1 minute

Speed gain: 10x faster on 1000x larger data!
```

### Real-world Analogy

```
Processing data on 1 computer = 1 person reading 1000 pages
Processing data on Spark = 100 people each reading 10 pages in parallel
Result: 100x faster! ✓
```

### Spark Architecture

```
┌─────────────────────────────────────────────────────┐
│            SPARK APPLICATION                        │
│  (Your Python code - office.py)                     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│         SPARK DRIVER (Coordinator)                  │
│  - Runs your main() function                        │
│  - Breaks job into tasks                            │
│  - Coordinates executors                            │
└─────────────────────────────────────────────────────┘
                        ↓
┌──────────────┬──────────────┬──────────────┐
│ EXECUTOR 1   │ EXECUTOR 2   │ EXECUTOR 3   │
│ (Worker 1)   │ (Worker 2)   │ (Worker 3)   │
│ 8GB RAM      │ 8GB RAM      │ 8GB RAM      │
│ Process data │ Process data │ Process data │
└──────────────┴──────────────┴──────────────┘
        ↓              ↓              ↓
  [Results combine at driver]
        ↓
   FINAL OUTPUT
```

---

## Topic 1.2: PySpark vs Spark

### Key Difference

```python
# Spark (Scala/Java)
val rdd = sc.textFile("path/file.txt")
val count = rdd.map(x => x.length).reduce((a,b) => a + b)

# PySpark (Python) - Same thing, easier syntax
rdd = sc.textFile("path/file.txt")
count = rdd.map(lambda x: len(x)).reduce(lambda a,b: a+b)
```

### Why PySpark?

- ✓ Python is easier than Scala/Java
- ✓ Huge data science ecosystem (pandas, numpy, scikit-learn)
- ✓ Most companies use it
- ✓ Your code is already in PySpark!

---

## Topic 1.3: Installation & Setup

### Step 1: Install Java (Required!)

```bash
# Windows
# Download from: https://www.oracle.com/java/technologies/downloads/#java11
java -version  # Verify installation

# Linux/Mac
sudo apt-get install openjdk-11-jdk  # or brew install openjdk@11
```

### Step 2: Install PySpark

```bash
pip install pyspark

# Verify
python -c "from pyspark.sql import SparkSession; print('PySpark OK!')"
```

### Step 3: Verify Installation

```bash
python << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
print("✓ Spark version:", spark.version)
print("✓ Spark home:", spark.sparkContext.getConf().get("spark.home"))
spark.stop()
EOF
```

---

## Topic 1.4: Your First Spark Application

### Example 1: Hello Spark

**File:** `lesson_1_hello_spark.py`

```python
// filepath: c:\Users\ABHI\Docer_Poject\lessons\lesson_1_hello_spark.py

from pyspark.sql import SparkSession

# Step 1: Create Spark Session (entry point)
spark = SparkSession.builder \
    .appName("HelloSpark") \
    # Use all available CPU cores on the local machine for parallel processing
    .master("local[*]") \
    .getOrCreate()

print("=" * 50)
print("WELCOME TO PYSPARK!")
print("=" * 50)

# Step 2: Create DataFrame (Spark's main data structure)
data = [
    ("Alice", 25, "NYC"),
    ("Bob", 30, "LA"),
    ("Charlie", 35, "Chicago")
]

df = spark.createDataFrame(data, ["Name", "Age", "City"])

# Step 3: Display data
print("\n✓ Created DataFrame:")
df.show()

# Step 4: Get info
print(f"\n✓ Row count: {df.count()}")
print(f"✓ Column count: {len(df.columns)}")
print(f"✓ Columns: {df.columns}")

# Step 5: Schema (data types)
print("\n✓ Schema:")
df.printSchema()

# Step 6: Stop Spark (cleanup)
spark.stop()
print("\n✓ Spark session stopped")
```

**Run it:**

```bash
python lesson_1_hello_spark.py
```

**Output:**

```
==================================================
WELCOME TO PYSPARK!
==================================================

✓ Created DataFrame:
+-------+---+--------+
|   Name|Age|    City|
+-------+---+--------+
|  Alice| 25|     NYC|
|    Bob| 30|      LA|
|Charlie| 35| Chicago|
+-------+---+--------+

✓ Row count: 3
✓ Column count: 3
✓ Columns: ['Name', 'Age', 'City']

✓ Schema:
root
 |-- Name: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- City: string (nullable = true)

✓ Spark session stopped
```

### Explanation

```python
spark = SparkSession.builder \
    .appName("HelloSpark") \      # Give app a name
    .master("local[*]") \         # Use all cores (local mode)
    .getOrCreate()                # Start session

# .master("local[*]") = Use laptop (development)
# .master("spark://cluster:7077") = Use cluster (production)
```

---

## Topic 1.5: Key Concepts

### 1. SparkSession

- **What:** Entry point to Spark
- **Analogy:** "Opening a connection to Spark"
- **Code:** `spark = SparkSession.builder...getOrCreate()`

### 2. DataFrame

- **What:** Distributed collection of data in columns
- **Analogy:** Like a huge Excel sheet across multiple computers
- **Code:** `df = spark.createDataFrame(data, columns)`

### 3. RDD (Resilient Distributed Dataset)

- **What:** Low-level data structure (legacy)
- **Modern:** Use DataFrames instead (easier, faster)
- **Skip for now:** Learn DataFrames first

### 4. Partitions

- **What:** DataFrame divided into chunks
- **Analogy:** If you have 1M rows and 10 partitions, each partition has ~100K rows
- **Why:** Each executor processes one partition in parallel

```
DataFrame: 1,000,000 rows

Without partitions:
  Executor 1: Process ALL 1M rows (slow, uses all memory)

With 10 partitions:
  Executor 1: Process partition 1 (100K rows)
  Executor 2: Process partition 2 (100K rows)
  ...
  Executor 10: Process partition 10 (100K rows)

Result: 10x faster! ✓
```

---

## PRACTICE EXERCISES: Level 1

### Exercise 1.1: Create & Explore DataFrame

```python
// filepath: c:\Users\ABHI\Docer_Poject\exercises\exercise_1_1.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex1_1").master("local[*]").getOrCreate()

# TODO: Create DataFrame with:
# - 5 students
# - Columns: Name, Age, Grade
# Example: ("John", 20, "A"), ("Jane", 21, "B"), ...

# TODO: Show the DataFrame
# TODO: Print row count
# TODO: Print schema
# TODO: Print column names

# BONUS: Find oldest student
# Hint: Use df.agg() or df.select()

spark.stop()
```

**Solution:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex1_1").master("local[*]").getOrCreate()

data = [
    ("John", 20, "A"),
    ("Jane", 21, "B"),
    ("Mike", 19, "A"),
    ("Sarah", 22, "C"),
    ("Tom", 20, "B")
]

df = spark.createDataFrame(data, ["Name", "Age", "Grade"])

print("DataFrame:")
df.show()

print(f"Row count: {df.count()}")
print(f"Columns: {df.columns}")

print("Schema:")
df.printSchema()

# Find oldest student
from pyspark.sql.functions import max
oldest = df.agg(max("Age")).collect()[0][0]
print(f"Oldest student age: {oldest}")

spark.stop()
```

---

# LEVEL 2: CORE OPERATIONS (Week 3-4)

## Topic 2.1: Read Data

### Reading Different Formats

**CSV Files:**

```python
// filepath: c:\Users\ABHI\Docer_Poject\lessons\lesson_2_read_data.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadData").master("local[*]").getOrCreate()

# Read CSV
df_csv = spark.read.option("header", "true").csv("path/to/file.csv")

# Read with schema inference
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("path/to/file.csv")

df.show()
spark.stop()
```

**JSON Files:**

```python
df_json = spark.read.json("path/to/file.json")
df_json.show()
```

**Parquet Files (Best for Big Data):**

```python
# Read
df_parquet = spark.read.parquet("path/to/file.parquet")

# Write
df.write.parquet("path/to/output.parquet")
```

**Text Files:**

```python
df_text = spark.read.text("path/to/file.txt")
# Returns DataFrame with one column: "value"
```

**From Database (Your Code):**

```python
df_db = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "table_name") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
```

---

## Topic 2.2: Basic DataFrame Operations

### Select Columns

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Operations").master("local[*]").getOrCreate()

# Sample data
data = [("Alice", 25, 60000), ("Bob", 30, 75000), ("Charlie", 35, 90000)]
df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# Select specific columns
df_selected = df.select("Name", "Age")
df_selected.show()
# Output:
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 25|
# |    Bob| 30|
# |Charlie| 35|
# +-------+---+

# Select all columns
df_all = df.select("*")

# Select with alias (rename)
from pyspark.sql.functions import col
df_renamed = df.select(col("Name").alias("Employee_Name"), col("Salary"))

spark.stop()
```

### Filter Rows

```python
# Keep rows where Age > 25
df_filtered = df.filter(df.Age > 25)
df_filtered.show()
# Output:
# +-------+---+------+
# |   Name|Age|Salary|
# +-------+---+------+
# |    Bob| 30| 75000|
# |Charlie| 35| 90000|
# +-------+---+------+

# Multiple conditions (AND)
df_filtered = df.filter((df.Age > 25) & (df.Salary > 70000))

# Multiple conditions (OR)
df_filtered = df.filter((df.Age < 25) | (df.Salary > 80000))
```

### Sort Data

```python
# Sort by Age ascending
df_sorted = df.sort("Age")

# Sort by Age descending
df_sorted = df.sort(df.Age.desc())

# Sort by multiple columns
df_sorted = df.sort("Age", "Salary")
```

---

## Topic 2.3: Aggregations

### Group By & Count

```python
from pyspark.sql.functions import count, sum as spark_sum, avg, max, min

# Create sample data
data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 6000),
    ("IT", "Charlie", 7000),
    ("IT", "David", 8000),
]
df = spark.createDataFrame(data, ["Department", "Name", "Salary"])

# Count employees per department
df_count = df.groupBy("Department").count()
df_count.show()
# Output:
# +----------+-----+
# |Department|count|
# +----------+-----+
# |     Sales|    2|
# |        IT|    2|
# +----------+-----+

# Sum salary per department
df_sum = df.groupBy("Department").agg(spark_sum("Salary").alias("Total_Salary"))
df_sum.show()
# Output:
# +----------+------------+
# |Department|Total_Salary|
# +----------+------------+
# |     Sales|       11000|
# |        IT|       15000|
# +----------+------------+

# Average, max, min
df_agg = df.groupBy("Department").agg(
    avg("Salary").alias("Avg_Salary"),
    max("Salary").alias("Max_Salary"),
    min("Salary").alias("Min_Salary")
)
df_agg.show()
```

---

## Topic 2.4: Add New Columns

### withColumn()

```python
from pyspark.sql.functions import col, lit, concat

# Add constant column
df_with_bonus = df.withColumn("Bonus", lit(1000))
# All rows get 1000 bonus

# Add calculated column
df_with_tax = df.withColumn("After_Tax", col("Salary") * 0.8)
# Salary * 0.8 (20% tax)

# Add concatenated column
df_with_fullinfo = df.withColumn(
    "Info",
    concat(col("Name"), lit(" - "), col("Department"))
)
df_with_fullinfo.show()
# Output:
# +-------+----------+----------+-----+------------------+
# |   Name|Department|    Salary|Bonus|              Info|
# +-------+----------+----------+-----+------------------+
# |  Alice|     Sales|      5000| 1000|  Alice - Sales   |
# +    Bob|     Sales|      6000| 1000|  Bob - Sales     |
# ...
```

---

## PRACTICE EXERCISES: Level 2

### Exercise 2.1: Real-World Example - Sales Data Analysis

```python
// filepath: c:\Users\ABHI\Docer_Poject\exercises\exercise_2_1.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ex2_1").master("local[*]").getOrCreate()

# Sample sales data
sales_data = [
    ("Alice", "2025-01-01", 100, "NYC"),
    ("Bob", "2025-01-01", 150, "LA"),
    ("Alice", "2025-01-02", 120, "NYC"),
    ("Charlie", "2025-01-02", 80, "Chicago"),
    ("Bob", "2025-01-03", 200, "LA"),
]

df = spark.createDataFrame(sales_data, ["Salesman", "Date", "Amount", "City"])

# TODO 1: Show all data
# TODO 2: Filter sales > 100
# TODO 3: Total sales per salesman
# TODO 4: Average sales per city
# TODO 5: Add commission column (10% of amount)
# TODO 6: Find salesman with highest total sales

spark.stop()
```

---

# LEVEL 3: INTERMEDIATE (Week 5-6)

## Topic 3.1: Joins

### Types of Joins

**Inner Join (Only matching rows):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Joins").master("local[*]").getOrCreate()

# Department data
dept_data = [("D1", "Sales"), ("D2", "IT"), ("D3", "HR")]
df_dept = spark.createDataFrame(dept_data, ["DeptID", "DeptName"])

# Employee data
emp_data = [("E1", "Alice", "D1"), ("E2", "Bob", "D2"), ("E3", "Charlie", "D4")]
df_emp = spark.createDataFrame(emp_data, ["EmpID", "Name", "DeptID"])

# Inner join (only D1, D2 match)
df_inner = df_emp.join(df_dept, on="DeptID", how="inner")
df_inner.show()
# Output:
# +------+-------+-------+--------+
# |DeptID| EmpID|   Name|DeptName|
# +------+-------+-------+--------+
# |    D1|     E1|  Alice|   Sales|
# |    D2|     E2|    Bob|     IT |
# +------+-------+-------+--------+
# (Charlie with D4 not shown - no match in dept)

# Left join (all employees, matching dept)
df_left = df_emp.join(df_dept, on="DeptID", how="left")
df_left.show()
# Output:
# +------+-------+-------+--------+
# |DeptID| EmpID|   Name|DeptName|
# +------+-------+-------+--------+
# |    D1|     E1|  Alice|   Sales|
# |    D2|     E2|    Bob|     IT |
# |    D4|     E3|Charlie|   NULL |
# +------+-------+-------+--------+
# (Charlie included with NULL dept)

spark.stop()
```

**Join Comparison:**

```
Inner Join:   Keep only MATCHING rows
Left Join:    Keep all LEFT rows + matching RIGHT
Right Join:   Keep all RIGHT rows + matching LEFT
Outer Join:   Keep all rows from both
```

---

## Topic 3.2: Window Functions

### Running Total Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Windows").master("local[*]").getOrCreate()

# Sample data
data = [
    ("Alice", 100),
    ("Bob", 150),
    ("Alice", 120),
    ("Bob", 200),
]
df = spark.createDataFrame(data, ["Name", "Amount"])

# Define window (partition by Name, ordered by rows)
window_spec = Window.partitionBy("Name").orderBy("Amount")

# Running total
df_with_running_total = df.withColumn(
    "Running_Total",
    sum("Amount").over(window_spec)
)
df_with_running_total.show()
# Output:
# +-----+------+---------------+
# | Name|Amount|Running_Total |
# +-----+------+---------------+
# |Alice|   100|            100|
# |Alice|   120|            220|
# |  Bob|   150|            150|
# |  Bob|   200|            350|
# +-----+------+---------------+
```

---

## Topic 3.3: UDFs (User Defined Functions)

### Create Custom Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF").master("local[*]").getOrCreate()

# Python function
def grade_student(score):
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    else:
        return "F"

# Register as UDF
grade_udf = udf(grade_student, StringType())

# Sample data
data = [("Alice", 95), ("Bob", 85), ("Charlie", 72)]
df = spark.createDataFrame(data, ["Name", "Score"])

# Apply UDF
df_with_grade = df.withColumn("Grade", grade_udf("Score"))
df_with_grade.show()
# Output:
# +-------+-----+-----+
# |   Name|Score|Grade|
# +-------+-----+-----+
# |  Alice|   95|    A|
# |    Bob|   85|    B|
# |Charlie|   72|    C|
# +-------+-----+-----+

spark.stop()
```

---

# LEVEL 4: ADVANCED (Week 7-8)

## Topic 4.1: Performance Optimization

### Partitioning

```python
# Partition by key column
df_partitioned = df.repartition(100, "Department")
# Creates 100 partitions, each with same department

# Write partitioned
df.write.partitionBy("Department").parquet("output_path")
# Automatically creates folder: Department=Sales/, Department=IT/, etc.
```

### Caching

```python
# Cache in memory (if reused multiple times)
df.cache()

# Now use df multiple times - reads from cache
result1 = df.filter(...).count()
result2 = df.filter(...).count()
# Both fast because df cached

# Remove from cache
df.unpersist()
```

### Broadcast Variables (For Your Code!)

```python
# Small table (< 100MB)
small_df = spark.read.format("jdbc").load()

# Broadcast to all executors (not driver)
from pyspark.sql.functions import broadcast

large_df.join(broadcast(small_df), ...)
# This is in your refactored code!
```

---

## Topic 4.2: Handling Large Data

### Lazy Evaluation

```python
# None of this executes yet!
df_filtered = df.filter(col("Age") > 25)
df_selected = df_filtered.select("Name", "Age")
df_sorted = df_selected.sort("Age")

# NOW it executes (action)
df_sorted.show()
```

**Why?** Spark optimizes the entire query before executing (Catalyst optimizer).

### Predicate Pushdown

```python
# Without optimization (slow):
# 1. Read 1TB from HDFS
# 2. Filter in Spark
# 3. Select columns

# Spark optimizes (fast):
# 1. Filter at source (HDFS only reads matching rows)
# 2. Select columns
# 3. Return small result

# You don't need to do anything - Spark does it automatically!
```

---

## Topic 4.3: SQL Queries

### Using SQL Instead of DataFrame API

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL").master("local[*]").getOrCreate()

data = [("Alice", 25, 60000), ("Bob", 30, 75000)]
df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# Register as temporary table
df.createOrReplaceTempView("employees")

# Use SQL
result = spark.sql("""
    SELECT Name, Salary * 1.1 as New_Salary
    FROM employees
    WHERE Age > 25
""")

result.show()
# Output:
# +-----+----------+
# | Name|New_Salary|
# +-----+----------+
# |  Bob|    82500 |
# +-----+----------+
```

**DataFrame API vs SQL:**

```python
# Same result, different style

# DataFrame API
df.filter(col("Age") > 25).select("Name", (col("Salary") * 1.1).alias("New_Salary"))

# SQL
spark.sql("SELECT Name, Salary * 1.1 as New_Salary FROM employees WHERE Age > 25")
```

---

# LEVEL 5: PRODUCTION PATTERNS (Week 9-10)

## Topic 5.1: Error Handling & Logging

### Logging (Your Code Uses This!)

```python
import logging
from pathlib import Path

def setup_logger(name: str) -> logging.Logger:
    """Setup production-grade logging."""
    Path("./logs").mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler("./logs/pipeline.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

logger = setup_logger(__name__)

logger.info("Pipeline started")
logger.warning("Processing slow")
logger.error("Error occurred")
```

### Try-Except

```python
try:
    batch_id = fetch_batch_id_safe(spark, pg_config)
    logger.info(f"✓ Batch ID: {batch_id}")
except Exception as e:
    logger.error(f"❌ Failed to fetch batch: {e}")
    raise  # Re-raise to stop pipeline
```

---

## Topic 5.2: Data Validation

### Schema Validation

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define expected schema
expected_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", IntegerType(), True),
])

# Read with schema
df = spark.read.schema(expected_schema).csv("file.csv")

# Validate
if df.schema == expected_schema:
    logger.info("✓ Schema valid")
else:
    logger.error("❌ Schema mismatch")
```

### Data Quality Checks

```python
# Check for nulls
null_count = df.filter(col("Name").isNull()).count()
if null_count > 0:
    logger.warning(f"⚠️  {null_count} null names found")

# Check count
if df.count() == 0:
    logger.error("❌ Empty DataFrame!")
    raise ValueError("No data")

# Check range
if df.agg(min("Age")).collect()[0][0] < 0:
    logger.error("❌ Negative age found!")
```

---

## Topic 5.3: Configuration Management

### Using Config Files

**config.json:**

```json
{
  "spark": {
    "executor_memory": "8g",
    "executor_cores": "4"
  },
  "database": {
    "url": "jdbc:postgresql://host:5432/db",
    "user": "fincore",
    "password": "Password#1234"
  }
}
```

**Python Code:**

```python
import json

with open("config.json") as f:
    config = json.load(f)

pg_config = config["database"]
# Use it
df = spark.read.format("jdbc") \
    .option("url", pg_config["url"]) \
    .load()
```

---

## COMPREHENSIVE EXAMPLE: Your GLIF Pipeline Explained

### Line-by-Line Walkthrough

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ========== STEP 1: Setup ==========
def setup_logger(name: str):
    """Production logging"""
    # Creates ./logs/pipeline.log
    # Logs all messages for debugging

def create_optimized_spark_session(app_name: str = "GLIF_Pipeline") -> SparkSession:
    """Create Spark with optimizations

    What each config does:
    - spark.executor.memory=8g: Each worker gets 8GB RAM
    - spark.shuffle.partitions=400: Optimal for your data volume
    - spark.sql.adaptive.enabled: Auto-optimize queries
    """

# ========== STEP 2: Fetch Data ==========
def fetch_batch_id_safe(spark, pg_config, max_retries=3):
    """Fetch batch ID with retry logic

    Why retry?
    - Network might timeout
    - Database might be busy
    - Exponential backoff: wait 1s, 2s, 4s before retrying
    """

def read_hdfs_safe(spark, hdfs_path, repartitions=200):
    """Read from HDFS with validation

    Repartition=200 means:
    - Split file into 200 chunks
    - Each executor processes one chunk in parallel
    - 200x parallelism!
    """

# ========== STEP 3: Parse Data (CODE 1) ==========
def process_glif_transactions(...):
    """
    Step A: Read fixed-width file
    Step B: Parse substring positions (positions 51, 48, 116, etc.)
    Step C: Decode OCR characters (p->0, q->1, etc.)
    Step D: Split into debits/credits
    Step E: Group by ID and sum
    Step F: Validate CGL codes with broadcast join
    Step G: Write to database
    Step H: Merge with yesterday's balance
    """
```

---

## QUICK REFERENCE: Common Operations

```python
# READ
df = spark.read.csv("file.csv")
df = spark.read.json("file.json")
df = spark.read.parquet("file.parquet")

# SELECT
df.select("col1", "col2")
df.select(df.col1, df.col2 * 2)

# FILTER
df.filter(col("Age") > 25)
df.filter((col("Dept") == "Sales") & (col("Salary") > 50000))

# GROUP & AGGREGATE
df.groupBy("Dept").agg(sum("Salary"), count("*"))

# JOIN
df1.join(df2, on="Key", how="inner")

# ADD COLUMN
df.withColumn("NewCol", col("OldCol") * 2)

# SORT
df.sort("Age")
df.sort(col("Age").desc())

# WRITE
df.write.csv("output")
df.write.parquet("output")
df.write.mode("append").csv("output")
```

---

## LEARNING SCHEDULE

| Week | Topics                              | Projects                   |
| ---- | ----------------------------------- | -------------------------- |
| 1-2  | Concepts, Session, DataFrame basics | Create sample DataFrame    |
| 3-4  | Read, Select, Filter, Aggregate     | Analyze sales data CSV     |
| 5-6  | Joins, Windows, UDFs                | Join employee + department |
| 7-8  | Performance, Optimization, SQL      | Optimize slow query        |
| 9-10 | Error handling, Logging, Config     | Production-ready pipeline  |

---

## NEXT: Hands-On Practice

Choose your learning style:

1. **Sequential:** Do lessons 1→2→3... in order
2. **Project-Based:** Pick a project and learn as needed
3. **Problem-Solving:** I give you data problems to solve

Which would you prefer?

---

## Resources

- [Official PySpark Docs](https://spark.apache.org/docs/latest/api/python/)
- [PySpark Examples GitHub](https://github.com/apache/spark/tree/master/examples/src/main/python)
- [Databricks Learning](https://databricks.com/learn)

---

## READY TO START?

Pick one:

1. ✅ Start with **Lesson 1.1** (Hello Spark)
2. ✅ Jump to specific topic
3. ✅ Ask me a question

What interests you most?
