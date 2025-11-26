# CGL Processing Pipeline - Complete Reference Guide

## Overview

This document captures the complete CGL (Cost General Ledger) processing pipeline for financial data validation, transformation, and reconciliation at scale (100M+ transaction rows).

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Data Pipeline Steps](#data-pipeline-steps)
3. [CGL Validation Logic](#cgl-validation-logic)
4. [Balancing & Synthetic Records](#balancing--synthetic-records)
5. [Code Patterns](#code-patterns)
6. [Performance Optimizations](#performance-optimizations)
7. [Common Errors & Fixes](#common-errors--fixes)

---

## Core Concepts

### What is CGL?

- **CGL (Cost General Ledger)**: A 10-digit code representing accounting cost centers/accounts
- **Valid CGL**: Present in the CGL Master (reference table)
- **Invalid CGL**: Not in master; must be replaced per business rules

### Debit & Credit Model

- **DEBIT_AMOUNT**: Positive values (additions to account)
- **CREDIT_AMOUNT**: Negative values (subtractions from account)
- **NET = DEBIT_AMOUNT + CREDIT_AMOUNT**: Should equal 0 for balanced transactions

### Balance Reconciliation

For every CGL group after aggregation, the sum of debits and credits must equal 0 (balanced books principle).

---

## Data Pipeline Steps

### Step 1: Load Raw Transaction Data

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, LongType
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.master("local[*]").appName("cgl_processing").getOrCreate()

schema = StructType([
    StructField("BATCH_ID", StringType(), True),
    StructField("JOURNAL_ID", StringType(), True),
    StructField("POST_DATE", TimestampType(), True),
    StructField("BRANCH_CODE", StringType(), True),
    StructField("CURRENCY", StringType(), True),
    StructField("CGL", StringType(), True),
    StructField("NARRATION", StringType(), True),
    StructField("DEBIT_AMOUNT", DecimalType(20,2), True),    # Always positive
    StructField("CREDIT_AMOUNT", DecimalType(20,2), True),   # Usually negative
    StructField("TRANSACTION_COUNT", LongType(), True),
    StructField("SOURCE_FLAG", StringType(), True)
])

df_transactions = spark.createDataFrame(data, schema)
```

**Key Points:**

- Use `Decimal(20,2)` for monetary amounts (precision over floats)
- `CREDIT_AMOUNT` is typically negative
- `TRANSACTION_COUNT` tracks transaction frequency for audit

---

### Step 2: Load CGL Master Reference

```python
from pyspark.sql.functions import broadcast, col, lit

schema_cgl = StructType([
    StructField("CGL_NUMBER", StringType(), True)
])

cgl_master = spark.createDataFrame(cgl_master_data, schema_cgl)

# Option A: Read from Oracle
# cgl_master = spark.read.format("jdbc") \
#     .option("url", oracle_url) \
#     .option("dbtable", "(SELECT CGL_NUMBER FROM cgl_master WHERE BAL_COMPARE=1)") \
#     .option("user", oracle_user) \
#     .option("password", oracle_password) \
#     .option("driver", oracle_driver) \
#     .load() \
#     .select(col("CGL_NUMBER").alias("CGL")).distinct()
```

**Key Points:**

- Use `broadcast()` for smaller reference tables (< 100MB typically)
- Distinct CGLs to avoid duplicate matches
- Can source from Oracle or static list

---

### Step 3: CGL Validation & Replacement

```python
from pyspark.sql.functions import when, broadcast, col, lit

df_validated = df_transactions.join(
    broadcast(cgl_master.select(col("CGL_NUMBER").alias("CGL")).withColumn("IS_VALID", lit(1))),
    on="CGL",
    how="left"
).withColumn(
    "VALIDATED_CGL",
    when(col("IS_VALID").isNotNull(), col("CGL"))                    # Case 1: Valid CGL → Keep it
    .when(col("CGL").startswith("5"), lit("5000000000"))             # Case 2: Starts with 5 → Replace with 5000000000
    .otherwise(lit("1111111111"))                                     # Case 3: Other invalid → Replace with 1111111111
)
```

**Logic Breakdown:**
| Case | Condition | Action | Example |
|------|-----------|--------|---------|
| 1 | CGL in Master | Keep original CGL | `2028070603` → `2028070603` |
| 2 | Starts with '5' & not in Master | Replace with `5000000000` | `5001234567` → `5000000000` |
| 3 | Other invalid | Replace with `1111111111` | `9999999999` → `1111111111` |

---

### Step 4: Aggregate by Validated CGL

```python
from pyspark.sql.functions import sum as spark_sum

result = df_validated.groupBy("VALIDATED_CGL").agg(
    spark_sum("DEBIT_AMOUNT").alias("TOTAL_DEBIT"),
    spark_sum("CREDIT_AMOUNT").alias("TOTAL_CREDIT"),
    spark_sum("TRANSACTION_COUNT").alias("TOTAL_TRANSACTIONS")
).orderBy("VALIDATED_CGL")

# Before balancing, most groups will have non-zero NET
# Example:
# VALIDATED_CGL | TOTAL_DEBIT | TOTAL_CREDIT | NET
# 1234567890    | 100.00      | -50.00       | 50.00  ← IMBALANCED
```

---

### Step 5: Identify Imbalanced Groups

```python
result_net = result.withColumn("NET", col("TOTAL_DEBIT") + col("TOTAL_CREDIT"))

imbalanced = result_net.filter(col("NET") != 0).select(
    "VALIDATED_CGL", "TOTAL_DEBIT", "TOTAL_CREDIT", "NET"
)
imbalanced.show()
```

---

### Step 6: Create Synthetic Balancing Records

```python
from pyspark.sql.functions import when, lit

synthetic = result_net.filter(col("NET") != 0).select(
    col("VALIDATED_CGL"),
    # If NET < 0 → Add debit to balance; else add 0
    when(col("NET") < 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_DEBIT"),
    # If NET > 0 → Add credit to balance; else add 0
    when(col("NET") > 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_CREDIT"),
    lit(0).cast(LongType()).alias("TOTAL_TRANSACTIONS")
)
```

**How it Works:**

| Scenario       | NET  | Action                      | Result                    |
| -------------- | ---- | --------------------------- | ------------------------- |
| DEBIT > CREDIT | +50  | Add synthetic: DR=0, CR=-50 | 100 + (-50) + (-50) = 0 ✓ |
| DEBIT < CREDIT | -100 | Add synthetic: DR=100, CR=0 | 100 + (-150) + 100 = 0 ✓  |
| DEBIT = CREDIT | 0    | No synthetic added          | 100 + (-100) = 0 ✓        |

---

### Step 7: Union Original + Synthetic and Re-Aggregate

```python
final_balanced = result.unionByName(synthetic)

final_agg = final_balanced.groupBy("VALIDATED_CGL").agg(
    spark_sum("TOTAL_DEBIT").alias("TOTAL_DEBIT"),
    spark_sum("TOTAL_CREDIT").alias("TOTAL_CREDIT"),
    spark_sum("TOTAL_TRANSACTIONS").alias("TOTAL_TRANSACTIONS")
).orderBy("VALIDATED_CGL")

# Verify balance
final_agg.withColumn("NET", col("TOTAL_DEBIT") + col("TOTAL_CREDIT")).show()
# All NET values should be 0.00
```

---

## CGL Validation Logic

### Decision Tree

```
START: For each CGL in raw data
│
├─ Is CGL in Master?
│  ├─ YES → VALIDATED_CGL = CGL (KEEP IT)
│  └─ NO → Go to next check
│
├─ Does CGL start with '5'?
│  ├─ YES → VALIDATED_CGL = '5000000000' (REPLACE)
│  └─ NO → Go to next check
│
└─ Default → VALIDATED_CGL = '1111111111' (REPLACE)

END: Use VALIDATED_CGL for all downstream processing
```

### Why This Logic?

- **Valid CGLs**: Preserve for audit trail and GL posting
- **Starting with 5**: Assumed to be cost center accounts → route to cost bucket `5000000000`
- **Others**: Default catch-all bucket `1111111111` for miscellaneous items

---

## Balancing & Synthetic Records

### Why Synthetic Records?

- Financial systems require balanced books (DR = CR)
- Raw transaction data may have timing/recording issues
- Synthetic records act as **audit-trail adjustments** to enforce balance

### Synthetic Record Creation Logic

```python
# Pseudo-code
FOR EACH cgl_group IN aggregated_result:
    net = group.total_debit + group.total_credit

    IF net == 0:
        # Already balanced
        skip

    ELIF net > 0:
        # More debits than credits
        # Add credit-only synthetic record to balance
        synthetic_record = {
            VALIDATED_CGL: cgl_group,
            TOTAL_DEBIT: 0,
            TOTAL_CREDIT: -net,
            TOTAL_TRANSACTIONS: 0
        }

    ELSE:  # net < 0
        # More credits than debits
        # Add debit-only synthetic record to balance
        synthetic_record = {
            VALIDATED_CGL: cgl_group,
            TOTAL_DEBIT: -net,
            TOTAL_CREDIT: 0,
            TOTAL_TRANSACTIONS: 0
        }
```

### Example Walkthrough

```
Original aggregation:
┌──────────────┬─────────────┬──────────────┐
│ VALIDATED_CGL│ TOTAL_DEBIT │ TOTAL_CREDIT │
├──────────────┼─────────────┼──────────────┤
│ 1234567890   │   1000.00   │    -600.00   │  NET = 400.00
│ 9876543210   │    500.00   │   -1000.00   │  NET = -500.00
│ 5555555555   │    200.00   │    -200.00   │  NET = 0.00
└──────────────┴─────────────┴──────────────┘

Synthetic records to add:
┌──────────────┬─────────────┬──────────────┐
│ VALIDATED_CGL│ TOTAL_DEBIT │ TOTAL_CREDIT │
├──────────────┼─────────────┼──────────────┤
│ 1234567890   │     0.00    │    -400.00   │  ← Offsets +400 net
│ 9876543210   │   500.00    │      0.00    │  ← Offsets -500 net
└──────────────┴─────────────┴──────────────┘

After union + re-aggregate:
┌──────────────┬─────────────┬──────────────┬─────┐
│ VALIDATED_CGL│ TOTAL_DEBIT │ TOTAL_CREDIT │ NET │
├──────────────┼─────────────┼──────────────┼─────┤
│ 1234567890   │   1000.00   │   -1000.00   │ 0.00│ ✓ BALANCED
│ 9876543210   │   1000.00   │   -1000.00   │ 0.00│ ✓ BALANCED
│ 5555555555   │    200.00   │    -200.00   │ 0.00│ ✓ BALANCED
└──────────────┴─────────────┴──────────────┴─────┘
```

---

## Code Patterns

### Pattern 1: Broadcast Join (Optimal for Reference Tables)

```python
from pyspark.sql.functions import broadcast

df_large.join(
    broadcast(df_small),
    on="key_column",
    how="left"
)

# Why broadcast?
# - df_small is duplicated to all executor nodes
# - Avoids expensive shuffle operation
# - Best when df_small < 100MB
# - Use with: reference tables, dimension tables, lookups
```

### Pattern 2: Conditional Column Creation

```python
from pyspark.sql.functions import when, col, lit

df.withColumn(
    "new_column",
    when(col("condition1"), value1)
    .when(col("condition2"), value2)
    .when(col("condition3"), value3)
    .otherwise(default_value)
)

# Equivalent to SQL CASE WHEN
```

### Pattern 3: Union & Re-aggregate

```python
df_final = (
    df_original.unionByName(df_synthetic)
    .groupBy("key")
    .agg(
        F.sum("metric1"),
        F.sum("metric2"),
        F.count("*")
    )
)

# When to use:
# - Adding synthetic rows before final aggregation
# - Ensures synthetic rows participate in totals
```

### Pattern 4: Decimal Type for Amounts

```python
from pyspark.sql.types import DecimalType

# ALWAYS use for monetary values:
StructField("amount", DecimalType(20, 2), True)

# Why not float?
# - Floats have precision errors (0.1 + 0.2 != 0.3)
# - DecimalType maintains exact precision for accounting
# - Matches database precision (e.g., Oracle NUMBER)
```

---

## Performance Optimizations

### 1. Broadcast Small Tables

```python
# ✓ GOOD - Reference table broadcasted
df_large.join(broadcast(df_reference), on="id", how="left")

# ✗ AVOID - No broadcast, triggers shuffle
df_large.join(df_reference, on="id", how="left")
```

**Impact:** 10x-100x faster for joins with reference tables.

### 2. Distinct Before Broadcast

```python
# ✓ Reduces broadcast size
broadcast(df_cgl.select("CGL").distinct())

# ✗ Broadcasts duplicates
broadcast(df_cgl)
```

### 3. Partition Early

```python
# For massive datasets (100M rows):
df.repartition(100)  # Adjust based on cluster size
  .groupBy("VALIDATED_CGL")
  .agg(...)
```

### 4. Use Appropriate Aggregation

```python
# ✓ FAST - Single pass aggregation
.agg(F.sum("amount"))

# ✗ SLOW - Multiple passes
.agg(F.sum("amount"), F.min("amount"), F.max("amount"))
# Better: .agg(F.sum(...), F.min(...), F.max(...)) in one call
```

### 5. Columnar Caching

```python
result = df.groupBy(...).agg(...)
result.cache()  # Keep in memory if reused multiple times
result.show()
result.describe()
```

---

## Common Errors & Fixes

### Error 1: Join Column Name Mismatch

```
AnalysisException: USING column `CGL` cannot be resolved on the right side
```

**Cause:** Right-side DataFrame has `CGL_NUMBER`, not `CGL`.

**Fix:**

```python
# ✓ CORRECT - Rename before join
cgl_master.select(col("CGL_NUMBER").alias("CGL"))
          .withColumn("IS_VALID", lit(1))

# Or use explicit join condition
df.join(cgl_master, df.CGL == cgl_master.CGL_NUMBER, how="left")
```

---

### Error 2: Type Mismatch in Union

```
AnalysisException: Cannot have multiple root data types
```

**Cause:** DataFrames in union have different column types.

**Fix:**

```python
# ✓ Use unionByName (matches columns by name, not order)
df1.unionByName(df2)

# ✓ Ensure schema matches
result.select(col("TOTAL_DEBIT").cast(DecimalType(20,2)))
```

---

### Error 3: Decimal Precision Loss

```python
# ✗ DON'T - Creates float, loses precision
data = [("1", 100.5, -50.25)]

# ✓ DO - Use Decimal explicitly
from decimal import Decimal
data = [("1", Decimal("100.50"), Decimal("-50.25"))]
```

---

### Error 4: Python Variable in Spark

```python
# ✗ WRONG - Breaks Spark optimization
window = 10
df.filter(col("amount") > window)  # window not serializable

# ✓ CORRECT - Use Spark literal
df.filter(col("amount") > lit(window))
```

---

## Configuration & Setup

### Spark Session for Local Development

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("cgl_processing") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Settings:
# - local[*]: Use all cores on machine
# - shuffle.partitions: Default 200, lower for dev (faster)
```

### Oracle Connection (Production)

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/db") \
    .option("dbtable", "schema.table") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("fetchsize", 10000) \
    .load()
```

---

## Checklist for New Pipeline Implementation

- [ ] Load raw data with correct schema (DecimalType for amounts)
- [ ] Load CGL Master reference table
- [ ] Broadcast join CGL Master with transaction data
- [ ] Apply CGL validation logic (if in master, keep; if starts with 5, replace with 5000000000; else 1111111111)
- [ ] Aggregate by VALIDATED_CGL (sum debits, credits, transaction count)
- [ ] Compute NET per group (debit + credit)
- [ ] Create synthetic balancing records for imbalanced groups
- [ ] Union synthetic records back to original aggregation
- [ ] Re-aggregate to final result
- [ ] Verify all NET values equal 0.00
- [ ] Export/write final result to target system (Oracle, file, data lake)

---

## Quick Reference Commands

```python
# View data with all columns
df.show(truncate=False)

# View schema
df.printSchema()

# Count rows
df.count()

# Get statistics
df.describe().show()

# Check for nulls
df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns]).show()

# Export to CSV
df.coalesce(1).write.mode("overwrite").csv("/path/to/output")

# Export to Parquet (recommended for reuse)
df.write.mode("overwrite").parquet("/path/to/output")

# Read Parquet
spark.read.parquet("/path/to/output").show()
```

---

## Related Files

- `newTask.py` - Working implementation with all steps
- `cgl_crdr.py` - Production-ready script with Oracle integration
- This file: `CGL_PROCESSING_NOTES.md` - Complete reference guide

---

**Last Updated:** November 27, 2025  
**Version:** 1.0  
**Status:** Complete & Production-Ready
