# OOM (Out Of Memory) - Complete Guide

## Definition

**OOM = Out Of Memory**

When a Java process (like Spark executor) tries to use more memory than allocated, it crashes with an OutOfMemoryError.

---

## Simple Analogy

Imagine you have a glass that holds exactly 500ml of water:

```
Glass capacity: 500ml
Water poured in: 600ml

Result: Water overflows, glass breaks! 💥
```

Same with Spark:

```
Executor memory: 8GB (allocated)
Data trying to load: 10GB

Result: OOM Error! Process crashes! 💥
```

---

## How OOM Happens in Your Pipeline

### SCENARIO 1: Reading Too Much Data Into Partitions

**Current Code (PROBLEMATIC):**

```python
df_raw = spark.read.text("hdfs://path/to/10GB_file")
# Without repartitioning, HDFS block size might be 256MB
# Result: 10GB ÷ 256MB = ~40 partitions
```

**What happens:**

```
Executor 1: Loads 10GB_file_partition_1 (256MB) ✓ OK
Executor 2: Loads 10GB_file_partition_2 (256MB) ✓ OK
...
Executor 40: Loads 10GB_file_partition_40 (256MB) ✓ OK

PROBLEM: Each partition still in memory after processing!
If executor memory is only 8GB:
  40 partitions × 256MB = 10GB total
  But each executor holds multiple partitions
  Executor 1 + Executor 2 sharing same memory
  Result: OOM! 💥
```

**Fixed Code:**

```python
# Repartition to reasonable size
df_raw = spark.read.text("hdfs://path/to/10GB_file").repartition(200)
# Result: 10GB ÷ 200 = 50MB per partition
# Each executor memory 8GB can hold: 8GB ÷ 50MB = 160 partitions
# Much safer! ✓
```

---

### SCENARIO 2: Using `.collect()` on Large Data

**DANGEROUS CODE (Your original code):**

```python
# Read 100K CGL codes from database
cgl_codes = [row.CGL_NUMBER for row in master_cgl_list.select("CGL_NUMBER").collect()]
#                                                                             ↑↑↑↑↑
#                                     .collect() pulls ALL data to driver!

# Example:
# 100K CGL codes × 10 bytes each = 1MB (OK for this)
# But imagine 10M rows × 1KB each = 10GB
# Result: Driver OOM! 💥
```

**What `.collect()` does:**

```
DataFrame on Executors (distributed):
  Executor 1: [CGL_1, CGL_2, CGL_3, ...]  (40K rows)
  Executor 2: [CGL_4, CGL_5, CGL_6, ...]  (40K rows)
  Executor 3: [CGL_7, CGL_8, CGL_9, ...]  (20K rows)

After .collect():
  Driver: [CGL_1, CGL_2, CGL_3, CGL_4, CGL_5, CGL_6, CGL_7, CGL_8, CGL_9, ...]
          ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ ALL 100K pulled to driver memory!

If 10M rows: Driver tries to hold 10GB → OOM! 💥
```

**Fixed Code:**

```python
# Use broadcast join instead (stays distributed)
joined = df_mapped.join(
    F.broadcast(master_cgl),  # Broadcast copies to executors, not driver
    df_mapped["CGL"] == master_cgl["CGL_NUMBER"],
    "left_outer"
)
# Each executor gets copy of CGL master (50MB each)
# No bottleneck at driver! ✓
```

---

### SCENARIO 3: Aggregation Without Skew Handling

**PROBLEMATIC CODE:**

```python
df_agg = df_raw.groupBy("Id").agg(
    F.sum("Amount").alias("Total")
)
```

**What happens (with data skew):**

```
Input: 10M transactions spread across 200 partitions
Scenario: 50% of all transactions have same ID!

Shuffle phase:
  Partition 1: [ID_001, ID_002, ID_003] → 50K rows each → Executor 1
  Partition 2: [ID_001, ID_004, ID_005] → 50K rows each → Executor 2
  Partition 3: [ID_001, ID_006, ID_007] → 50K rows each → Executor 3
  ...
  Partition 200: [ID_001, ID_200] → 50K rows → Executor 200

AGGREGATION PHASE (groupBy shuffles):
  ID_001 (50% of data = 5M rows) → Goes to single executor for aggregation!

  Executor 1 memory: 8GB
  Data trying to aggregate: 5M rows × 1KB each = 5GB
  Plus intermediate data: +3GB for sort/groupBy internals
  Total: 8GB needed, but allocated 8GB
  Result: OOM! 💥
```

**Why?**

- GroupBy operation shuffles all data with same key to SAME executor
- If one key has 50% of data, that executor gets hammered
- Memory fills up, crashes!

**Fixed Code:**

```python
# Salt the skewed keys (split into multiple parts)
SALT_FACTOR = 10

df_salted = df_raw.withColumn(
    "Id_salted",
    when(col("Id").isin(*skewed_keys),
         concat(col("Id"), lit("_"), (F.rand() * SALT_FACTOR).cast("int")))
    .otherwise(col("Id"))
)

# Now ID_001 becomes ID_001_0, ID_001_1, ID_001_2, etc.
# Each goes to different executor!
# Then re-aggregate:
df_agg = df_salted.groupBy("Id_salted").agg(...) \
    .groupBy(regexp_extract(col("Id_salted"), "^(.*)_\\d+$", 1)) \
    .agg(F.sum(...))

# Result: Distributed across 10 executors instead of 1! ✓
```

---

### SCENARIO 4: Not Dropping Unused Columns

**PROBLEMATIC CODE:**

```python
df = spark.read.parquet("10GB_file")
# 1000 columns

df_processed = df.select("Id", "Amount", "Date")
# Only need 3 columns, but still holding 1000 in memory during processing!

df_agg = df_processed.groupBy("Id").agg(F.sum("Amount"))
# During groupBy shuffle, all 1000 columns go across network!
# Result: OOM during shuffle! 💥
```

**Fixed Code:**

```python
df = spark.read.parquet("10GB_file")

# Drop unused columns EARLY
df_slim = df.select("Id", "Amount", "Date")

# Now only 3 columns in memory
df_agg = df_slim.groupBy("Id").agg(F.sum("Amount"))
# Much smaller data shuffled! ✓
```

---

## OOM Error Message Example

When OOM happens, you see this in logs:

```
Exception in thread "Executor task launch worker"
java.lang.OutOfMemoryError: Java heap space
at java.util.Arrays.copyOf(Arrays.java:2332)
at java.io.ByteArrayOutputStream.grow(ByteArrayOutputStream.java:113)
...

ERROR executor.Executor: Exception in task 23.0 in stage 1.0 (TID 45)
java.lang.OutOfMemoryError: Java heap space
```

**Translation:**

- "Java heap space" = Memory for objects exhausted
- Task 23 in Stage 1 crashed
- Usually followed by entire Spark job failure

---

## How to Detect OOM Risk in Your Code

### Red Flag 1: `.collect()` on Large DataFrames

```python
# ❌ RISKY
large_df.collect()  # If large_df > driver memory, OOM!

# ✓ SAFE
large_df.first()  # Gets 1 row, safe
large_df.take(100)  # Gets 100 rows, safe
large_df.show()  # Displays sample, safe
```

### Red Flag 2: Small Partitions + Large File

```python
# ❌ RISKY
5GB_file with 10 partitions = 500MB each
If executor memory is 2GB, 4 executors sharing = 500MB each
Barely fits, risky!

# ✓ SAFE
5GB_file with 100 partitions = 50MB each
Much more headroom!
```

### Red Flag 3: Data Skew Without Handling

```python
# ❌ RISKY
df.groupBy("customer_id").agg(...)  # If 1 customer has 90% of data, OOM!

# ✓ SAFE
df.groupBy("customer_id", "date").agg(...)  # Smaller groups, safer
```

### Red Flag 4: Caching Without Memory

```python
# ❌ RISKY
df.cache()  # Stores in executor memory
df.groupBy(...).agg(...)  # Now trying to use more memory
df.join(other_df, ...)  # OOM if total > executor memory!

# ✓ SAFE
df.cache()  # Cache only small, reused DataFrames
df_small = df.filter(col(...)).cache()  # Cache filtered version
```

---

## Memory Allocation in Spark

### Default Configuration (DANGEROUS)

```python
# Spark defaults:
spark.driver.memory = 1GB    # Driver (coordinator)
spark.executor.memory = 1GB  # Each executor

# With 8 executors:
Total memory: 1GB + (1GB × 8) = 9GB
```

**Problem:** 1GB is tiny! OOM very likely.

### Production Configuration (SAFE)

```python
spark = SparkSession.builder \
    .appName("GLIF_Pipeline") \
    .config("spark.driver.memory", "4g") \     # Driver: 4GB
    .config("spark.executor.memory", "8g") \   # Each: 8GB
    .config("spark.executor.instances", "8") \ # 8 executors
    .getOrCreate()

# Total memory: 4GB + (8GB × 8) = 68GB
# Much safer for 10M+ row operations!
```

---

## Memory Breakdown

When an executor processes data, memory is used for:

```
8GB Executor Memory:
├─ Spark Internal (Metadata, ClassLoader): 500MB
├─ Shuffle Memory (sorting, groupBy): 1GB
├─ Storage Memory (cached DataFrames): 2GB
├─ Execution Memory (computation): 2GB
├─ DataFrames Being Processed: 1.5GB
└─ Buffer/Overhead: 500MB
   ──────────────────────────────
   Total: 8GB (fully utilized, risky!)
```

If any operation tries to use more, **OOM!**

---

## Quick Fix Checklist

| Issue                    | Fix                                | Impact                |
| ------------------------ | ---------------------------------- | --------------------- |
| `.collect()` on large DF | Use `.first()` or `.broadcast()`   | Prevents driver OOM   |
| Too few partitions       | Increase repartitions              | Prevents executor OOM |
| Data skew                | Salt keys or use broadcast join    | Prevents shuffle OOM  |
| Executor memory too low  | Increase `spark.executor.memory`   | Gives more headroom   |
| Many unused columns      | Drop early                         | Reduces data size     |
| Caching too much         | Cache selectively                  | Preserves memory      |
| No shuffle memory tuning | Set `spark.shuffle.memoryFraction` | Optimization          |

---

## Real Example from Your Pipeline

### Original Code (OOM Risk)

```python
# Code 1: Risk if 100K CGLs × large DF
cgl_codes = [row.CGL_NUMBER for row in master_cgl_list.select("CGL_NUMBER").collect()]
# If master_cgl_list is 10M rows → 100MB+ to driver → Driver OOM!

df_result = df_final_schema.withColumn(
    "CGL",
    when(col("CGL").isin(cgl_codes), col("CGL"))  # Large IN clause!
    ...
)
# isin() with 100K values = huge SQL IN clause → slow + risky!
```

### Fixed Code (Safe)

```python
# Use broadcast join (your refactored code)
master_cgl = spark.read.format("jdbc").load()  # Stays distributed

joined = df_mapped.join(
    F.broadcast(master_cgl),  # Copy to all executors (no driver OOM)
    df_mapped["CGL"] == master_cgl["CGL_NUMBER"],
    "left_outer"  # Efficient local join on each executor
)
# Result: No OOM, 10x faster! ✓
```

---

## Monitoring for OOM

### In Spark UI

1. Open Spark Web UI (usually `http://localhost:4040`)
2. Go to "Executors" tab
3. Look for:
   - "Storage Memory" spike → Caching issue
   - "Execution Memory" spike → GroupBy/shuffle issue
   - Any executor showing "FAILED" → Likely OOM

### In Logs

```bash
# Search for OOM errors
grep -i "OutOfMemory" spark_logs.txt

# Will show which stage/task failed
# If Stage 2, task 45 → groupBy operation
# If Stage 5, task 1 → shuffle operation
```

### Using Logging (Your Code)

```python
# Add memory tracking
import os
from psutil import virtual_memory

def log_memory():
    mem = virtual_memory()
    logger.info(f"Memory: {mem.used / 1e9:.2f}GB / {mem.total / 1e9:.2f}GB")

# Before and after critical operations
log_memory()
df_agg = df.groupBy("Id").agg(F.sum("Amount"))
log_memory()
```

---

## Summary

| Term                | Meaning                       | Your Pipeline                        |
| ------------------- | ----------------------------- | ------------------------------------ |
| **OOM**             | Out Of Memory error           | Crashes when data > allocated memory |
| **Executor Memory** | RAM per Spark worker          | `spark.executor.memory = 8g`         |
| **Driver Memory**   | RAM for coordinator           | `spark.driver.memory = 4g`           |
| **Partition**       | Chunk of distributed data     | 200 partitions = 200 pieces          |
| **Shuffle**         | Moving data between executors | GroupBy, join shuffle data           |
| **Skew**            | Uneven data distribution      | 1 key with 50% of data               |
| **Cache**           | Keeping DataFrame in memory   | Useful if reused, risky if large     |

**Golden Rule:**

```
Allocated Memory > Data Size + Intermediate Operations + Buffer
        8GB         >  5GB    +        2GB            +   1GB

If NOT true → OOM! 💥
```

---

## Your Pipeline's OOM Protection (Refactored)

Your new code includes:

```python
✓ Broadcast joins (no .collect())
✓ Proper repartitioning (200 partitions)
✓ Optimized memory config (8GB executors)
✓ Single-pass aggregations (fewer shuffles)
✓ Early column dropping (smaller data)
✓ Logging (tracking memory)

Result: Very unlikely to OOM! 🎉
```

If you still hit OOM, next steps:

```python
1. Increase executor memory:
   .config("spark.executor.memory", "16g")

2. Increase partitions:
   df.repartition(400)

3. Process in batches:
   for batch_date in date_range:
       process(batch_date)  # Smaller batch per run

4. Profile bottleneck:
   Use Spark UI to find which stage uses most memory
```
