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

# broadcast join - you already used this idea; keep it
joined_df = df_mapped.join(F.broadcast(master_cgl_list),
                          df_mapped["CGL"] == master_cgl_list["CGL_NUMBER"],
                          "left_outer")

# validated cgl: if no master match then apply rules
df_valid = joined_df.withColumn(
    "Validated_CGL",
    when(col("CGL_NUMBER").isNull(),
         when(substring(col("CGL"), 1, 1) == lit("5"),
              lit("5000000000")).otherwise(lit("1111111111"))
    ).otherwise(col("CGL_NUMBER"))
)

df_valid1 = df_valid.select(
    "BATCH_ID", "JOURNAL_ID", "POST_DATE", "BRANCH_CODE", "CURRENCY",
    F.col("Validated_CGL").alias("CGL"),
    "NARRATION", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG"
)

# ---------- summary aggregation (group-level) ----------
df_summary = df_valid1.groupBy("CGL", "BRANCH_CODE", "CURRENCY").agg(
    F.sum("DEBIT_AMOUNT").alias("Total_Aggregated_DEBIT"),
    F.sum("CREDIT_AMOUNT").alias("Total_Aggregated_CREDIT"),
    F.sum("TRANSACTION_COUNT").alias("Total_Transaction_Count")
)

join_keys = ["CGL", "BRANCH_CODE", "CURRENCY"]

df_final_with_all_details = df_valid1.join(df_summary, on=join_keys, how="left") \
    .withColumn("TRANSACTION_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

# ---------- compute processed balances to merge with GL_BALANCE ----------
# Here we compute net balance from the processed file: sum(debits) + sum(credits)
processed_balances = df_final_with_all_details.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(
    (F.coalesce(F.sum("DEBIT_AMOUNT"), F.lit(0)) + F.coalesce(F.sum("CREDIT_AMOUNT"), F.lit(0))).alias("BALANCE")
)

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
