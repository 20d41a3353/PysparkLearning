# GLIF Pipeline — Detailed Operation Walkthrough

## Overview

This pipeline reads fixed-width financial transaction files from HDFS, parses them, validates accounts (CGLs), aggregates balances, and writes results to Oracle database.

---

## SECTION 1: Spark Session & Configuration Setup

### 1.1 Create Spark Session

```python
spark = SparkSession.builder \
    .appName("HDFS_Text_to_Oracle_Optimized") \
    .config("spark.jars", "./ojdbc8.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022") \
    .getOrCreate()
```

**What it does:**

- Creates a distributed Spark cluster session for parallel processing
- Loads Oracle JDBC driver (`ojdbc8.jar`) to enable DB connections
- Sets default HDFS URI for all read operations

**Example:**

```
✓ SparkSession initialized with name: HDFS_Text_to_Oracle_Optimized
✓ Driver: ojdbc8.jar loaded
✓ HDFS Namenode: hdfs://10.177.103.199:8022
```

---

### 1.2 Apply Performance Tunings

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

**What each does:**

| Config                    | Purpose                                     | Impact                                  |
| ------------------------- | ------------------------------------------- | --------------------------------------- |
| `adaptive.enabled`        | Auto-adjust partitions based on query stats | 20-30% faster joins                     |
| `shuffle.partitions=400`  | Default partitions for distributed ops      | Prevents too-few partitions causing OOM |
| `arrow.pyspark.enabled`   | Use Arrow for Python↔JVM serialization      | 10x faster data transfer                |
| `maxPartitionBytes=256MB` | Max bytes per partition                     | Larger files = fewer partitions         |

**Example Impact:**

```
Without tuning:  10 partitions × 5GB file = 500MB each, some executor OOMs(Out Of Memory)
With tuning:     400 partitions × 5GB file = 12.5MB each, balanced load
Result:          3 min → 1 min execution
```

---

## SECTION 2: Fetch Batch ID from Oracle

### 2.1 JDBC Read from Oracle

```python
df_batch_id = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("query", "SELECT get_next_batch_id AS BATCH_ID_VAL FROM DUAL") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("fetchsize", 1) \
    .load()

batch_row = df_batch_id.first()
BATCH_ID_LITERAL = int(batch_row["BATCH_ID_VAL"])
```

**What it does:**

- Calls Oracle function `get_next_batch_id()` to get next unique batch number
- Returns a DataFrame with 1 row
- Extracts the value using `.first()`

**Example:**

```
Input SQL:    SELECT get_next_batch_id AS BATCH_ID_VAL FROM DUAL
Oracle DB:    Sequence value = 50042
Result DF:
  BATCH_ID_VAL
  50042

BATCH_ID_LITERAL = 50042

Output: "=== Fetched BATCH_ID: 50042 ==="
```

**Why `.first()` instead of `.collect()[0]`?**

- `.first()`: Stops after 1 row (efficient)
- `.collect()[0]`: Pulls entire result to driver (wasteful for large datasets)

---

## SECTION 3: Read HDFS Files

### 3.1 Read Text Files

```python
hdfs_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/VARAGA_GLIFONL_35*"
df_raw = spark.read.text(hdfs_path).repartition(READ_REPARTITIONS)
```

**What it does:**

- Reads all files matching glob pattern `VARAGA_GLIFONL_35*` from HDFS
- Each line becomes one row with column `value` (String)
- Repartitions to 200 partitions for parallel processing

**Example HDFS Structure:**

```
/CBS-FILES/2025-11-20/GLIF/
  ├── VARAGA_GLIFONL_350
  ├── VARAGA_GLIFONL_351
  ├── VARAGA_GLIFONL_352
  └── VARAGA_GLIFONL_353
```

**Example Raw Data (1 line = 1 transaction record):**

```
value (raw fixed-width string, 180+ chars)
─────────────────────────────────────────────────────────────────────────────
" 2025112001234567890123456789INRDEBIT000123456789012345678901234567890DEP..."
```

**After read.text():**

```
+─────────────────────────────────────────────────────────────────────────────+
| value                                                                       |
+─────────────────────────────────────────────────────────────────────────────+
| 2025112001234567890123456789INRDEBIT000123456789012345678901234567890DEP... |
| 2025112000987654321098765432INRxxxxDEBIT000987654321098765432101234567xxx... |
+─────────────────────────────────────────────────────────────────────────────+
```

**Repartition effect:**

```
Before:   20 partitions (HDFS block size)
After:    200 partitions (parallelized across 200 executor tasks)
Result:   Faster parsing + aggregation
```

---

## SECTION 4: Parse Fixed-Width Record Format

### 4.1 Extract Substrings

```python
df_parsed = df_raw.select(
    substring("value", 51, 18).alias("Id"),                    # Pos 51-68
    substring("value", 48, 3).alias("currency_code"),          # Pos 48-50
    when(substring("value", 48, 3) != "INR", substring("value", 133, 17))
    .otherwise(substring("value", 116, 17)).alias("Amount_raw"), # Conditional
    when(substring("value", 48, 3) != "INR", substring("value", 149, 1))
    .otherwise(substring("value", 132, 1)).alias("sign_char")   # Conditional
).drop("value")
```

**What it does:**

- Extracts fixed-width fields by position and length
- Uses conditional logic: different positions for INR vs non-INR (Foreign Currency)
- Drops original `value` column to save memory

**Example Parsing:**

Raw string (positions marked):

```
Position:    1         10        20        30        40        50        60
             |---------|---------|---------|---------|---------|---------|--
value:       "  2025112           03001    INR     1000000000123456789012345"
```

Breaking it down:

```
substring(value, 51, 18)          → "0000123456789012345"  (Id)
substring(value, 48, 3)           → "INR"                  (currency_code)

Since currency = "INR":
  substring(value, 116, 17)       → "00000000000050000p"   (Amount_raw)
  substring(value, 132, 1)        → "p"                    (sign_char)

Result after select:
┌────────────────────┬───────────────┬──────────────────┬────────┐
│ Id                 │ currency_code │ Amount_raw       │ sign   │
├────────────────────┼───────────────┼──────────────────┼────────┤
│ 0000123456789012345│ INR           │ 00000000000050000p│ p      │
└────────────────────┴───────────────┴──────────────────┴────────┘
```

**Why conditional logic?**

- **INR (Indian Rupees):** Amount at position 116-132
- **USD/EUR (Foreign Currency):** Amount at position 133-149
- Different banks encode currency amounts at different positions

---

### 4.2 Split Amount into Base and Sign

```python
df_parsed = df_parsed.withColumn("Amount_base", substring("Amount_raw", 1, 16))
```

**What it does:**

- Takes first 16 chars of 17-char amount field (the digits)
- Last character (17th) is the sign indicator

**Example:**

```
Amount_raw:        "00000000000050000p"
                    |_______________| |
                    16 chars (base)  1 char (sign)

Amount_base:       "0000000000050000"
sign_char:         "p"
```

---

### 4.3 Decode OCR Sign Characters

```python
last_digit_expr = when(col("sign_char").rlike("^[p-y]$"),
                       translate(col("sign_char"), "pqrstuvwxy", "0123456789")
                      ).otherwise(col("sign_char"))

sign_expr = when(col("sign_char").isin('p','q','r','s','t','u','v','w','x','y'), -1).otherwise(1)
```

**What it does:**

- Maps OCR-encoded characters to digits: `p→0, q→1, r→2, ..., y→9`
- Letters `p-y` indicate negative amount; digits indicate positive

**Example Mapping:**

| sign_char | Decoded Digit | Sign Multiplier | Meaning                          |
| --------- | ------------- | --------------- | -------------------------------- |
| `p`       | 0             | -1              | Negative amount, last digit is 0 |
| `q`       | 1             | -1              | Negative amount, last digit is 1 |
| `8`       | 8             | +1              | Positive amount, last digit is 8 |
| `9`       | 9             | +1              | Positive amount, last digit is 9 |

**Example Calculation:**

```
Amount_base:         "0000000000050000"
sign_char:           "p"
Decoded digit:       "0"
Sign multiplier:     -1

Amount_decimal_str = concat("0000000000050000", "0")
                   = "00000000000500000"

Amount_final = (00000000000500000 ÷ 1000) × (-1)
             = 50000 × (-1)
             = -50000  (NEGATIVE amount)
```

---

### 4.4 Split into Positive & Negative Columns

```python
df_parsed = df_parsed.withColumn("Amountpve",
                 when(col("Amount_final") > 0, col("Amount_final")).otherwise(F.lit(0))) \
                .withColumn("Amountnve",
                 when(col("Amount_final") < 0, col("Amount_final")).otherwise(F.lit(0)))
```

**What it does:**

- Creates separate columns for debits (positive) and credits (negative)
- Useful for separate aggregation: total debits vs total credits

**Example:**

```
Amount_final = -50000

Amountpve = when(-50000 > 0, -50000).otherwise(0) = 0
Amountnve = when(-50000 < 0, -50000).otherwise(0) = -50000

Result:
┌──────────────┬──────────────┐
│ Amountpve    │ Amountnve    │
├──────────────┼──────────────┤
│ 0            │ -50000       │
└──────────────┴──────────────┘
```

---

## SECTION 5: Aggregate Transactions by Account ID

### 5.1 Group and Sum

```python
df_agg = df_parsed.groupBy("Id").agg(
    F.sum("Amountpve").alias("DEBIT_AMOUNT"),
    F.sum("Amountnve").alias("CREDIT_AMOUNT"),
    F.count(lit(1)).alias("TRANSACTION_COUNT")
)
```

**What it does:**

- Groups all rows with same `Id` (transaction account)
- Sums debits and credits separately
- Counts transaction records per ID

**Example Input (1000 rows, same ID):**

```
Id                  Amount_final  Amountpve   Amountnve
────────────────────────────────────────────────────────
0000123456789012345 +50000        50000       0
0000123456789012345 -25000        0           -25000
0000123456789012345 +100000       100000      0
0000123456789012345 -15000        0           -15000
...999 more rows...
```

**After groupBy("Id").agg():**

```
┌────────────────────┬──────────────┬──────────────┬──────────────────┐
│ Id                 │ DEBIT_AMOUNT │ CREDIT_AMOUNT│ TRANSACTION_COUNT│
├────────────────────┼──────────────┼──────────────┼──────────────────┤
│ 0000123456789012345│ 250000000    │ -80000000    │ 1000             │
└────────────────────┴──────────────┴──────────────┴──────────────────┘
```

**Benefit:**

- Reduces 1000 rows → 1 row per account
- Significantly reduces data size before DB write

---

## SECTION 6: Map to Final GL Transaction Schema

### 6.1 Transform to GL_TRANSACTIONS Format

```python
df_mapped = df_agg.select(
    F.lit(str(BATCH_ID_LITERAL)).alias("BATCH_ID"),
    F.lit(None).cast(StringType()).alias("JOURNAL_ID"),
    to_timestamp(F.lit(posting_date_str), "yyyy-MM-dd").alias("POST_DATE"),
    substring("Id", 1, 5).alias("BRANCH_CODE"),
    substring("Id", 6, 3).alias("CURRENCY"),
    substring("Id", 9, 10).alias("CGL"),
    F.lit("CBS consolidated txns").cast(StringType()).alias("NARRATION"),
    col("DEBIT_AMOUNT"),
    col("CREDIT_AMOUNT"),
    col("TRANSACTION_COUNT"),
    F.lit("C").cast(StringType()).alias("SOURCE_FLAG")
)
```

**What it does:**

- Extracts branch, currency, account from transaction ID
- Adds metadata (batch ID, posting date, narration, source flag)
- Creates columns matching GL_TRANSACTIONS table schema

**Example Transformation:**

Input row:

```
Id                   DEBIT_AMOUNT  CREDIT_AMOUNT  TRANSACTION_COUNT
0000123456789012345  250000000     -80000000      1000
 │││││              │││││
 └─────┘             └─────┘
 Positions 1-5       Positions 6-8    Positions 9-18
 BRANCH_CODE         CURRENCY        CGL
```

Output row after mapping:

```
BATCH_ID  JOURNAL_ID  POST_DATE            BRANCH_CODE CURRENCY CGL        NARRATION              DEBIT_AMOUNT CREDIT_AMOUNT TRANSACTION_COUNT SOURCE_FLAG
50042     NULL        2025-11-20 00:00:00  00001       123      6789012345 CBS consolidated txns 250000000    -80000000     1000              C
```

---

## SECTION 7: Validate CGL (Account) Codes

### 7.1 Read CGL Master Table

```python
cgl_query = "(SELECT CGL_NUMBER FROM cgl_master where BAL_COMPARE=1) T1"
master_cgl_list = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", cgl_query) \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("fetchsize", BATCHSIZE_JDBC) \
    .load() \
    .select("CGL_NUMBER") \
    .distinct()
```

**What it does:**

- Reads list of valid CGL codes from Oracle
- Applies filter: `BAL_COMPARE=1` (only balance-comparable accounts)
- `.distinct()` removes duplicates

**Example CGL Master Table (Oracle):**

```
CGL_NUMBER
──────────────
1000000001
1000000002
1000000100
1000010001
...
5000000000  (catch-all for invalid 5xxx codes)
```

---

### 7.2 Broadcast Join

```python
joined_df = df_mapped.join(F.broadcast(master_cgl_list),
                          df_mapped["CGL"] == master_cgl_list["CGL_NUMBER"],
                          "left_outer")
```

**What it does:**

- Broadcasts master_cgl_list to all executors (small table)
- Joins on CGL code with left outer join (keeps all df_mapped rows)
- Marks matching rows with `CGL_NUMBER` (NULL if no match)

**Broadcast vs Shuffle Join:**

```
Normal Join (shuffle):
  Partition 1: [CGL 1000, 1001, 1002]  →  Network shuffle to CGL master  → 10 secs
  Partition 2: [CGL 2000, 2001, 2002]  →  Network shuffle to CGL master  → 10 secs
  Partition 3: [CGL 3000, 3001, 3002]  →  Network shuffle to CGL master  → 10 secs
  Total: 10 + 10 + 10 = 30 secs

Broadcast Join (F.broadcast):
  Master sent to all executors once: 2 secs
  Partition 1: [CGL 1000, 1001, 1002]  ✓  Local join with master  → 1 sec
  Partition 2: [CGL 2000, 2001, 2002]  ✓  Local join with master  → 1 sec
  Partition 3: [CGL 3000, 3001, 3002]  ✓  Local join with master  → 1 sec
  Total: 2 + 1 + 1 + 1 = 5 secs  ← 6x faster!
```

**Example Result:**

```
Before join (df_mapped):
┌────────┬────────────┐
│ CGL    │ DEBIT_AMNT │
├────────┼────────────┤
│1000000 │ 50000      │  ← Valid CGL
│9999999 │ 25000      │  ← Invalid CGL
│5000123 │ 10000      │  ← Invalid 5xxx
└────────┴────────────┘

After join (left_outer):
┌────────┬────────────┬────────────┐
│ CGL    │ DEBIT_AMNT │ CGL_NUMBER │
├────────┼────────────┼────────────┤
│1000000 │ 50000      │ 1000000    │  ← Matched
│9999999 │ 25000      │ NULL       │  ← No match
│5000123 │ 10000      │ NULL       │  ← No match
└────────┴────────────┴────────────┘
```

---

### 7.3 Apply Validation Rules

```python
df_valid = joined_df.withColumn(
    "Validated_CGL",
    when(col("CGL_NUMBER").isNull(),
         when(substring(col("CGL"), 1, 1) == lit("5"),
              lit("5000000000")).otherwise(lit("1111111111"))
    ).otherwise(col("CGL_NUMBER"))
)
```

**What it does:**

- If CGL_NUMBER matched (not NULL): keep it
- If no match AND starts with "5": default to "5000000000" (expense catch-all)
- If no match AND NOT "5": default to "1111111111" (asset catch-all)

**Example Application:**

```
Input rows:
┌────────┬────────────┐
│ CGL    │ CGL_NUMBER │
├────────┼────────────┤
│1000000 │ 1000000    │  ← Valid match
│9999999 │ NULL       │  ← Invalid, doesn't start with 5
│5000123 │ NULL       │  ← Invalid, starts with 5
└────────┴────────────┘

After validation:
┌────────┬────────────┬─────────────────┐
│ CGL    │ CGL_NUMBER │ Validated_CGL   │
├────────┼────────────┼─────────────────┤
│1000000 │ 1000000    │ 1000000         │  ← Keep valid
│9999999 │ NULL       │ 1111111111      │  ← Route to asset catch-all
│5000123 │ NULL       │ 5000000000      │  ← Route to expense catch-all
└────────┴────────────┴─────────────────┘
```

**Why these defaults?**

- `5xxxxxxxx`: Expense/P&L accounts; `5000000000` = general P&L holding
- `1xxxxxxxx`: Asset accounts; `1111111111` = general asset holding
- Ensures no invalid CGLs break database constraints

---

## SECTION 8: Compute Summary Aggregation

### 8.1 Group by Branch/Currency/CGL

```python
df_summary = df_valid1.groupBy("CGL", "BRANCH_CODE", "CURRENCY").agg(
    F.sum("DEBIT_AMOUNT").alias("Total_Aggregated_DEBIT"),
    F.sum("CREDIT_AMOUNT").alias("Total_Aggregated_CREDIT"),
    F.sum("TRANSACTION_COUNT").alias("Total_Transaction_Count")
)
```

**What it does:**

- Re-aggregates at branch level (one row per branch-currency-cgl combo)
- Provides summary statistics for reconciliation

**Example:**

Before (detail level):

```
BRANCH CURRENCY CGL      DEBIT_AMT
00001  INR      1000000  100000
00001  INR      1000000  50000
00001  INR      2000000  75000
```

After (summary):

```
BRANCH CURRENCY CGL      Total_Aggregated_DEBIT
00001  INR      1000000  150000
00001  INR      2000000  75000
```

---

### 8.2 Enrich Original Data with Summary

```python
df_final_with_all_details = df_valid1.join(df_summary, on=join_keys, how="left") \
    .withColumn("TRANSACTION_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
```

**What it does:**

- Joins summary stats back to detail records
- Each detail row now has its branch-level totals
- Adds posting date

---

## SECTION 9: Compute Processed Balance Deltas

### 9.1 Calculate Net Balance Change

```python
processed_balances = df_final_with_all_details.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(
    (F.coalesce(F.sum("DEBIT_AMOUNT"), F.lit(0)) +
     F.coalesce(F.sum("CREDIT_AMOUNT"), F.lit(0))).alias("BALANCE")
)
```

**What it does:**

- Groups by account (CGL, currency, branch)
- Sums debits + credits = net balance change from today's transactions
- `coalesce()` handles NULLs (treats as 0)

**Example Calculation:**

```
Transactions for CGL 1000000, INR, Branch 00001:
  Debit  Credit
  100000 -50000  (= -50000)
  50000  -25000  (= -25000)
  ───────────────
  Totals:
  DEBIT_AMOUNT = 150000
  CREDIT_AMOUNT = -75000

BALANCE = (150000 + (-75000)) = 75000

Result:
CGL      CURRENCY BRANCH BALANCE
1000000  INR      00001  75000
```

---

## SECTION 10: Read Previous Day's Balance

### 10.1 Oracle Query with Date Filter

```python
filter_date_str = yesterday.strftime("%Y-%m-%d")
sql_query = f"""(
    SELECT CGL,BALANCE,CURRENCY,BRANCH_CODE
    FROM GL_BALANCE
    WHERE TRUNC(BALANCE_DATE) = TO_DATE('{filter_date_str}', 'YYYY-MM-DD')
) T1"""

df_date_filtered = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", sql_query) \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("fetchsize", BATCHSIZE_JDBC) \
    .load()
```

**What it does:**

- Queries GL_BALANCE table for yesterday's date
- `TRUNC()` removes time component (compares dates only)
- Reads into Spark DataFrame

**Example Query Execution:**

```
filter_date_str = "2025-11-19"  (yesterday)

SQL becomes:
  SELECT CGL,BALANCE,CURRENCY,BRANCH_CODE
  FROM GL_BALANCE
  WHERE TRUNC(BALANCE_DATE) = TO_DATE('2025-11-19', 'YYYY-MM-DD')

Oracle returns:
CGL      BALANCE  CURRENCY BRANCH_CODE
1000000  500000   INR      00001
1000001  750000   INR      00001
...

Spark DataFrame:
+────────+────────+────────+──────────+
|CGL     |BALANCE |CURRENCY|BRANCH_CD |
+────────+────────+────────+──────────+
|1000000 |500000  |INR     |00001     |
|1000001 |750000  |INR     |00001     |
+────────+────────+────────+──────────+
```

---

## SECTION 11: Combine Today's and Yesterday's Balances

### 11.1 Union DataFrames

```python
df_date_filtered = df_date_filtered.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE")

combined_df = processed_balances.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE") \
              .unionByName(df_date_filtered)
```

**What it does:**

- Ensures both DataFrames have identical columns in same order
- `.unionByName()` combines rows from both
- Total rows = today's transactions + yesterday's balance

**Example:**

Today's processed balances:

```
CGL      CURRENCY BRANCH BALANCE
1000000  INR      00001  75000
1000001  INR      00001  50000
```

Yesterday's GL_BALANCE:

```
CGL      CURRENCY BRANCH BALANCE
1000000  INR      00001  500000
1000002  INR      00001  100000
```

After union:

```
CGL      CURRENCY BRANCH BALANCE
1000000  INR      00001  75000      ← Today
1000001  INR      00001  50000      ← Today
1000000  INR      00001  500000     ← Yesterday
1000002  INR      00001  100000     ← Yesterday
```

Notice: CGL 1000000 appears twice (today's delta + yesterday's balance)

---

### 11.2 Final Aggregation (Re-aggregate)

```python
final_aggregated_df = combined_df.groupBy("CGL", "CURRENCY", "BRANCH_CODE") \
    .agg(F.sum("BALANCE").alias("BALANCE")) \
    .orderBy("CGL", "BRANCH_CODE") \
    .withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
```

**What it does:**

- Groups by account (CGL, currency, branch)
- Sums all balances (today's delta + yesterday's balance = cumulative)
- Sorts by CGL and branch for consistency
- Adds posting date for tracking

**Example Final Result:**

```
From combined (4 rows above):
┌────────┬────────┬───────┬─────────┐
│ CGL    │ CURR   │ BRANCH│ BALANCE │
├────────┼────────┼───────┼─────────┤
│1000000 │ INR    │ 00001 │ 75000   │  }
│1000000 │ INR    │ 00001 │ 500000  │  } Same CGL, sum = 575000
│1000001 │ INR    │ 00001 │ 50000   │
│1000002 │ INR    │ 00001 │ 100000  │
└────────┴────────┴───────┴─────────┘

After final group + sum:
┌────────┬────────┬───────┬────────────┬─────────────────┐
│ CGL    │ CURR   │ BRANCH│ BALANCE    │ BALANCE_DATE    │
├────────┼────────┼───────┼────────────┼─────────────────┤
│1000000 │ INR    │ 00001 │ 575000     │ 2025-11-20      │
│1000001 │ INR    │ 00001 │ 50000      │ 2025-11-20      │
│1000002 │ INR    │ 00001 │ 100000     │ 2025-11-20      │
└────────┴────────┴───────┴────────────┴─────────────────┘
```

**Calculation Breakdown for CGL 1000000:**

```
Yesterday's balance:     500000
Today's delta:          + 75000
───────────────────────────────
Today's closing balance: 575000
```

---

## SECTION 12: Write to Oracle Database

### 12.1 Repartition for Parallel Writes

```python
df_to_write = df_final_with_all_details.repartition(WRITE_PARTITIONS, "BRANCH_CODE") \
                 .select("BATCH_ID", "JOURNAL_ID", "POST_DATE", "BRANCH_CODE", "CURRENCY", "CGL",
                         "NARRATION", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG")
```

**What it does:**

- Repartitions to 20 partitions (WRITE_PARTITIONS)
- Groups by BRANCH_CODE (co-locates branch data)
- Selects only columns needed for GL_TRANSACTIONS table

**Benefit of Repartitioning:**

```
Before repartition (1 partition):
  Executor 1: Write 1M rows to Oracle (slow, single connection)

After repartition(20):
  Executor 1: Write 50K rows (partition 1) ────┐
  Executor 2: Write 50K rows (partition 2) ────┼→ Oracle 20 parallel connections
  ...                                           │
  Executor 20: Write 50K rows (partition 20) ──┘
  Result: 20x parallel writes, 3-5x faster
```

---

### 12.2 JDBC Write to GL_TRANSACTIONS

```python
df_to_write.write \
    .format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "GL_TRANSACTIONS") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("batchsize", BATCHSIZE_JDBC) \
    .mode("append") \
    .save()
```

**What it does:**

- Opens JDBC connections to Oracle
- Converts Spark rows to JDBC batches (110K rows per batch)
- Inserts into GL_TRANSACTIONS table
- `.mode("append")` adds rows without truncating

**Example JDBC Batching:**

```
Total rows to write: 550,000
Batch size: 110,000

Batch 1: rows 1-110,000      → INSERT
Batch 2: rows 110,001-220,000 → INSERT
Batch 3: rows 220,001-330,000 → INSERT
Batch 4: rows 330,001-440,000 → INSERT
Batch 5: rows 440,001-550,000 → INSERT

Total JDBC round-trips: 5 (vs 550,000 individual INSERT statements)
```

---

### 12.3 JDBC Write to GL_BALANCE

```python
final_aggregated_df.repartition(WRITE_PARTITIONS).write \
    .format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "GL_BALANCE") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("batchsize", BATCHSIZE_JDBC) \
    .mode("append") \
    .save()
```

**What it does:**

- Similar to GL_TRANSACTIONS write
- Writes aggregated balances (fewer rows: 1 per account)
- Enables date-based queries (GL_BALANCE for each day)

---

## SECTION 13: Error Handling

### 13.1 Try-Except Wrapper

```python
try:
    df_to_write.write \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", "GL_TRANSACTIONS") \
        ...
        .save()
    print("=== GL_TRANSACTIONS written successfully ===")
except Exception as e:
    print(f"Error writing GL_TRANSACTIONS: {e}")
```

**What it does:**

- Catches any errors during JDBC write
- Prints error message for debugging
- Allows pipeline to continue (or fail gracefully)

**Common Errors:**

| Error                           | Cause                          | Solution                    |
| ------------------------------- | ------------------------------ | --------------------------- |
| `Connection refused`            | Oracle not running/unreachable | Verify Oracle host/port     |
| `Invalid username/password`     | Wrong credentials              | Update config               |
| `Table not found`               | Table doesn't exist            | Create table in Oracle      |
| `ORA-00001: Duplicate key`      | Unique constraint violated     | Truncate table or use merge |
| `ORA-12514: Listener not found` | SID/service name wrong         | Check Oracle SID            |

---

## SECTION 14: Full Pipeline Execution Summary

### 14.1 Data Flow Diagram

```
HDFS Files (fixed-width)
        ↓
    [READ]
        ↓
Raw DataFrame (1 row = 1 transaction record)
        ↓
    [PARSE] (substring, decode OCR, split pos/neg)
        ↓
Parsed DataFrame (cleaned fields, signed amounts)
        ↓
    [AGGREGATE by ID]
        ↓
Aggregated DF (1 row = 1 account)
        ↓
    [MAP to GL schema]
        ↓
Mapped DF (with BATCH_ID, dates, etc)
        ↓
    [VALIDATE CGLs] (broadcast join against master)
        ↓
Validated DF (invalid CGLs remapped to defaults)
        ↓
    [SPLIT]
    ├─→ Write GL_TRANSACTIONS table (detail)
    └─→ Compute balance deltas
            ↓
        [UNION with yesterday's GL_BALANCE]
            ↓
        [RE-AGGREGATE]
            ↓
        Final GL_BALANCE (cumulative)
            ↓
        Write GL_BALANCE table (summary)
```

### 14.2 Row Count Example (10M input records)

```
Stage                          Input Rows    Output Rows   Reduction
─────────────────────────────────────────────────────────────────────
1. Read HDFS                      10,000,000  10,000,000
2. Parse                          10,000,000  10,000,000   0%
3. Aggregate by ID                10,000,000     100,000   99%
4. Map to GL schema                 100,000     100,000   0%
5. Validate CGL                     100,000     100,000   0%
6. Write GL_TRANSACTIONS            100,000  ✓ Written
7. Compute balance deltas           100,000       2,500   97.5%
8. Union with yesterday              2,500       3,500   (added yesterday's records)
9. Re-aggregate                      3,500       2,500   28.5%
10. Write GL_BALANCE                 2,500  ✓ Written

Compression: 10M → 100K (GL_TRANSACTIONS) and 2.5K (GL_BALANCE)
Execution time: ~5-10 minutes (cluster size dependent)
```

---

## SECTION 15: Performance Tips

### 15.1 Optimize for Your Cluster

```python
READ_REPARTITIONS = 200      # Match number of executor cores
WRITE_PARTITIONS = 20        # Conservative, avoid overwhelming Oracle
BATCHSIZE_JDBC = 110000      # Balance: memory vs round-trips
```

| Parameter         | Low Value            | High Value            | Trade-off                            |
| ----------------- | -------------------- | --------------------- | ------------------------------------ |
| READ_REPARTITIONS | Few large partitions | Many small partitions | OOM vs network overhead              |
| WRITE_PARTITIONS  | Few connections      | Many connections      | Single bottleneck vs Oracle overload |
| BATCHSIZE_JDBC    | More JDBC calls      | Fewer JDBC calls      | Network latency vs memory            |

### 15.2 Avoid Common Mistakes

```python
# ❌ DON'T:
cgl_codes = master_cgl.collect()  # Collects entire table to driver (OOM)

# ✓ DO:
joined = df.join(F.broadcast(master_cgl), ...)  # Broadcast to executors
```

```python
# ❌ DON'T:
df.count()  # Full scan, forces materialization

# ✓ DO:
df.cache()  # If reusing DataFrame multiple times
df.count()  # Now count is instant (cached)
```

---

## Conclusion

The pipeline transforms raw fixed-width transaction files into reconcilable GL balances through:

1. Parsing and cleaning
2. Aggregation and validation
3. Balance computation and merging
4. Parallel database writes

**Key optimizations:**

- Broadcast joins (avoid shuffles)
- Single-pass aggregations
- Repartitioned parallel writes
- Conditional logic based on currency type
- Graceful error handling

This design processes millions of transactions efficiently while maintaining data accuracy and auditability.
