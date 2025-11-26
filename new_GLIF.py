import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import expr, col, when, substring, concat, lit, to_timestamp, translate
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType
from datetime import date, timedelta

# ---------- configuration / tuning ----------
READ_REPARTITIONS = 200      # tune by cluster cores & input size
WRITE_PARTITIONS = 20        # tune by Oracle concurrency you want
BATCHSIZE_JDBC = 110000

today = date.today()
yesterday = today - timedelta(days=1)
posting_date_str = today.strftime("%Y-%m-%d")

spark = SparkSession.builder \
    .appName("HDFS_Text_to_Oracle_Optimized") \
    .config("spark.jars", "./ojdbc8.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022") \
    .getOrCreate()

# Adaptive + shuffle tuning (keep, already good)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# ---------- Oracle JDBC ----------
oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1"
oracle_user = "fincore"
oracle_password = "Password#1234"
oracle_driver = "oracle.jdbc.driver.OracleDriver"

# fetch batch id (use .first() rather than full collect)
try:
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
    print(f"=== Fetched BATCH_ID: {BATCH_ID_LITERAL} ===")
except Exception as e:
    print(f"Error fetching BATCH_SEQ via JDBC: {e}")
    spark.stop()
    raise

# ---------- read HDFS file ----------
hdfs_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/VARAGA_GLIFONL_35*"
df_raw = spark.read.text(hdfs_path).repartition(READ_REPARTITIONS)

# Parse fields in a single select step (fewer logical plans)
df_parsed = df_raw.select(
    substring("value", 51, 18).alias("Id"),
    substring("value", 48, 3).alias("currency_code"),
    # choose substring locations based on currency
    when(substring("value", 48, 3) != "INR", substring("value", 133, 17))
    .otherwise(substring("value", 116, 17)).alias("Amount_raw"),
    # sign char is last char of the 17-char amount
    when(substring("value", 48, 3) != "INR", substring("value", 149, 1))
    .otherwise(substring("value", 132, 1)).alias("sign_char")
).drop("value")

# split base and sign (Amount_raw has 17 chars: first 16 = base, 17th = sign/digit)
df_parsed = df_parsed.withColumn("Amount_base", substring("Amount_raw", 1, 16))

# Map sign_char -> last_digit (letters p..y map to 0..9). Use translate for single-char mapping.
# sign (positive/negative) is determined by letters p..y being negative (as in original logic).
# For letter -> digit mapping: p->0, q->1, ..., y->9
last_digit_expr = when(col("sign_char").rlike("^[p-y]$"),
                       translate(col("sign_char"), "pqrstuvwxy", "0123456789")
                      ).otherwise(col("sign_char"))

sign_expr = when(col("sign_char").isin('p','q','r','s','t','u','v','w','x','y'), -1).otherwise(1)

df_parsed = df_parsed.withColumn("last_digit", last_digit_expr) \
    .withColumn("sign", sign_expr) \
    .withColumn("Amount_decimal_str", concat(col("Amount_base"), col("last_digit")))

# cast and scale once
df_parsed = df_parsed.withColumn("Amount_final",
                                 (col("Amount_decimal_str").cast(DecimalType(22, 3)) / F.lit(1000)) * col("sign")
                                )

# positive / negative splits
df_parsed = df_parsed.withColumn("Amountpve", when(col("Amount_final") > 0, col("Amount_final")).otherwise(F.lit(0))) \
                     .withColumn("Amountnve", when(col("Amount_final") < 0, col("Amount_final")).otherwise(F.lit(0)))

# aggregate transactions per Id (map key)
df_agg = df_parsed.groupBy("Id").agg(
    F.sum("Amountpve").alias("DEBIT_AMOUNT"),
    F.sum("Amountnve").alias("CREDIT_AMOUNT"),
    F.count(lit(1)).alias("TRANSACTION_COUNT")
)

# map to final columns in a single select
df_mapped = df_agg.select(
    F.lit(str(BATCH_ID_LITERAL)).alias("BATCH_ID").cast(StringType()),
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

df_final_schema = df_mapped.select(
    "BATCH_ID",
    "JOURNAL_ID",
    "POST_DATE",
    "BRANCH_CODE",
    "CURRENCY",
    "CGL",
    "NARRATION",
    "DEBIT_AMOUNT",
    "CREDIT_AMOUNT",
    "TRANSACTION_COUNT",
    "SOURCE_FLAG"
)

# ---------- read CGL master and join (broadcasted) ----------
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

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, LongType
# from datetime import datetime
# from decimal import Decimal

# spark = SparkSession.builder.master("local[*]").appName("newTask").getOrCreate()

# schema = StructType([
#     StructField("BATCH_ID", StringType(), True),
#     StructField("JOURNAL_ID", StringType(), True),
#     StructField("POST_DATE", TimestampType(), True),
#     StructField("BRANCH_CODE", StringType(), True),
#     StructField("CURRENCY", StringType(), True),
#     StructField("CGL", StringType(), True),
#     StructField("NARRATION", StringType(), True),
#     StructField("DEBIT_AMOUNT", DecimalType(20,2), True),
#     StructField("CREDIT_AMOUNT", DecimalType(20,2), True),
#     StructField("TRANSACTION_COUNT", LongType(), True),
#     StructField("SOURCE_FLAG", StringType(), True)
# ])

# data = [
#     ("252", None, datetime(2025,11,25), "15779", "INR", "5106505001", "CBS consolidated txns", Decimal("4633292.34"), Decimal("-2304292.55"), 2293, "C"),
#     ("252", None, datetime(2025,11,25), "06851", "INR", "1106505001", "CBS consolidated txns", Decimal("4708426.21"), Decimal("-2789806.95"), 2810, "C"),
#     ("252", None, datetime(2025,11,25), "04545", "INR", "1106505001", "CBS consolidated txns", Decimal("405.00"), Decimal("-10731.06"), 11, "C"),
#     ("252", None, datetime(2025,11,25), "32291", "INR", "2028070603", "CBS consolidated txns", Decimal("55651.00"), Decimal("-426620.92"), 83, "C"),
#     ("252", None, datetime(2025,11,25), "16000", "INR", "1106505001", "CBS consolidated txns", Decimal("100.00"), Decimal("-34136.00"), 6, "C"),
#     ("252", None, datetime(2025,11,25), "06659", "INR", "2014070602", "CBS consolidated txns", Decimal("90694.20"), Decimal("-70364.20"), 192, "C"),
#     ("252", None, datetime(2025,11,25), "11223", "INR", "5001234567", "CBS consolidated txns", Decimal("12345.67"), Decimal("-23456.78"), 45, "C"),
#     ("252", None, datetime(2025,11,25), "99887", "INR", "6200123456", "CBS consolidated txns", Decimal("74620.50"), Decimal("-12800.30"), 210, "C"),
#     ("252", None, datetime(2025,11,25), "44556", "INR", "1204500001", "CBS consolidated txns", Decimal("998.00"), Decimal("-1200.45"), 17, "C"),
#     ("252", None, datetime(2025,11,25), "88771", "INR", "6363123456", "CBS consolidated txns", Decimal("25000.75"), Decimal("-46000.00"), 92, "C"),
# ]


# df_10 = spark.createDataFrame(data, schema)
# df_10.show(truncate=False)

# # Create a single-column DataFrame for CGL codes: use one-element tuples
# schemacode = StructType([
#     StructField("CGL", StringType(), True)
# ])

# # Each element must be a tuple for a single-field schema (note trailing comma)
# codes = [
#     ("2028070603",),
#     ("2014070602",),
#     ("5001234567",),
#     ("6200123456",),
#     ("1204500001",),
# ]

# df_codes = spark.createDataFrame(codes, schemacode)
# df_codes.show(truncate=False)

print("\n" + "="*80)
print("Step 1: Left join df_10 with df_codes on CGL")
print("="*80)

# Left join df_10 with df_codes to mark valid CGLs
# Use broadcast for df_codes since it's much smaller (10k rows vs 100M rows)
from pyspark.sql.functions import broadcast, when, col

df_merged = df_final_schema.join(
    broadcast(master_cgl_list.select("CGL_NUMBER").distinct().withColumn("IS_VALID", col("CGL"))),
    on="CGL",
    how="left"
)

print("\nMerged DataFrame (first 5 rows):")
df_merged.show(truncate=False)

print("\n" + "="*80)
print("Step 2: Add VALIDATED_CGL column with conditional replacement")
print("="*80)

# Add VALIDATED_CGL column:
# - If CGL is in df_codes (IS_VALID is not null), keep original CGL
# - Else if CGL starts with '5', replace with '5000000000'
# - Else replace with '1111111111'
df_validated = df_merged.withColumn(
    "VALIDATED_CGL",
    when(col("IS_VALID").isNotNull(), col("CGL"))
    .when(col("CGL").startswith("5"), "5000000000")
    .otherwise("1111111111")
)

print("\nValidated DataFrame (first 10 rows):")
df_validated.select("CGL", "IS_VALID", "VALIDATED_CGL").show(truncate=False)

print("\n" + "="*80)
print("Step 3: GroupBy VALIDATED_CGL and aggregate")
print("="*80)

# Group by VALIDATED_CGL and sum DEBIT_AMOUNT, CREDIT_AMOUNT, TRANSACTION_COUNT
from pyspark.sql.functions import sum as spark_sum

result = df_validated.groupBy("VALIDATED_CGL").agg(
    spark_sum("DEBIT_AMOUNT").alias("TOTAL_DEBIT"),
    spark_sum("CREDIT_AMOUNT").alias("TOTAL_CREDIT"),
    spark_sum("TRANSACTION_COUNT").alias("TOTAL_TRANSACTIONS")
).orderBy("VALIDATED_CGL")

print("\nFinal Aggregated Result:")
result.show(truncate=False)
# +-------------+-----------+------------+------------------+
# |VALIDATED_CGL|TOTAL_DEBIT|TOTAL_CREDIT|TOTAL_TRANSACTIONS|
# +-------------+-----------+------------+------------------+
# |1111111111   |4733931.96 |-2880674.01 |2919              |
# |1204500001   |998.00     |-1200.45    |17                |
# |2014070602   |90694.20   |-70364.20   |192               |
# |2028070603   |55651.00   |-426620.92  |83                |
# |5000000000   |4633292.34 |-2304292.55 |2293              |
# |5001234567   |12345.67   |-23456.78   |45                |
# |6200123456   |74620.50   |-12800.30   |210               |
# +-------------+-----------+------------+------------------+ 

print("\nSummary:")
print(f"Total unique validated CGLs: {result.count()}")
result.describe().show()
# +-------+--------------------+-----------------+------------------+------------------+
# |summary|       VALIDATED_CGL|      TOTAL_DEBIT|      TOTAL_CREDIT|TOTAL_TRANSACTIONS|
# +-------+--------------------+-----------------+------------------+------------------+
# |  count|                   7|                7|                 7|                 7|
# |   mean| 3.222730048571429E9|   1371647.667143|    -817058.458571| 822.7142857142857|
# | stddev|2.1058210677401948E9|2262912.303230421|1233060.3711987433|1233.5967350645446|
# |    min|          1111111111|           998.00|       -2880674.01|                17|
# |    max|          6200123456|       4733931.96|          -1200.45|              2919|
# +-------+--------------------+-----------------+------------------+------------------+

print("\n" + "="*80)
print("Step 4: Add synthetic balancing records where needed and re-aggregate")
print("="*80)

from pyspark.sql.functions import lit

# Compute net = TOTAL_DEBIT + TOTAL_CREDIT (credit values are negative)
result_net = result.withColumn("NET", col("TOTAL_DEBIT") + col("TOTAL_CREDIT"))

print("\nGroups with non-zero net (before balancing):")
result_net.filter(col("NET") != 0).select("VALIDATED_CGL", "TOTAL_DEBIT", "TOTAL_CREDIT", "NET").show(truncate=False)

# Create synthetic rows to zero-out the net per group:
# - If NET > 0 -> add a credit row with TOTAL_DEBIT=0, TOTAL_CREDIT = -NET
# - If NET < 0 -> add a debit row with TOTAL_DEBIT = -NET, TOTAL_CREDIT=0
synthetic = result_net.filter(col("NET") != 0).select(
    col("VALIDATED_CGL"),
    when(col("NET") < 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_DEBIT"),
    when(col("NET") > 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_CREDIT"),
    lit(0).cast(LongType()).alias("TOTAL_TRANSACTIONS")
)

print("\nSynthetic balancing rows to add:")
synthetic.show(truncate=False)

# Union synthetic rows back to the aggregated result and re-aggregate
final = result.unionByName(synthetic)
# .groupBy("VALIDATED_CGL").agg(
#     spark_sum("TOTAL_DEBIT").alias("TOTAL_DEBIT"),
#     spark_sum("TOTAL_CREDIT").alias("TOTAL_CREDIT"),
#     spark_sum("TOTAL_TRANSACTIONS").alias("TOTAL_TRANSACTIONS")
# ).orderBy("VALIDATED_CGL")

print("\nFinal Aggregated Result After Balancing:")
final.show(truncate=False)






# ---------- read GL_BALANCE for the filter date ----------
filter_date_str = yesterday.strftime("%Y-%m-%d")  # explicit date string
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

# ensure identical schema/column order for union
df_date_filtered = df_date_filtered.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE")

# ---------- combine current processed balances with existing balances and aggregate ----------
combined_df = processed_balances.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE").unionByName(df_date_filtered)

final_aggregated_df = combined_df.groupBy("CGL", "CURRENCY", "BRANCH_CODE") \
    .agg(F.sum("BALANCE").alias("BALANCE")) \
    .orderBy("CGL", "BRANCH_CODE") \
    .withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

#---------- write outputs ----------
# Write GL_TRANSACTIONS (example) - tune partitions for parallel write but avoid overwhelming Oracle
try:
    # if you want parallel JDBC writers, repartition first; be conservative with the number
    df_to_write = df_final_with_all_details.repartition(WRITE_PARTITIONS, "BRANCH_CODE") \
                     .select("BATCH_ID", "JOURNAL_ID", "POST_DATE", "BRANCH_CODE", "CURRENCY", "CGL",
                             "NARRATION", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG")

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

    print("=== GL_TRANSACTIONS written successfully ===")
except Exception as e:
    print(f"Error writing GL_TRANSACTIONS: {e}")

# Write GL_BALANCE
try:
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

    print("=== GL_BALANCE written successfully ===")
except Exception as e:
    print(f"Error writing GL_BALANCE: {e}")

spark.stop()
