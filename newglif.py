import datetime 
from pyspark.sql import SparkSession , functions as F
from pyspark.sql.functions import expr, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType 
from datetime import date, timedelta

# Get today's date
today = date.today()

# Calculate yesterday's date
# timedelta(days=1) represents a difference of one day
yesterday = today - timedelta(days=1)

spark = SparkSession.builder \
    .appName("HDFS_Text_to_Oracle") \
    .config("spark.jars", "./ojdbc8.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022") \
    .getOrCreate()
    
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# oracle_url = "jdbc:oracle:thin:@//10.191.216.58:1523/crsprod"
# oracle_user = "ftwoahm"
# oracle_password = "Password@123"
# oracle_driver = "oracle.jdbc.driver.OracleDriver"

oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1"
oracle_user = "fincore"
oracle_password = "Password#1234"
oracle_driver = "oracle.jdbc.driver.OracleDriver"

DriverManager = spark._jvm.java.sql.DriverManager


batch_id_schema = StructType([
    StructField("BATCH_ID_VAL", LongType(), False) 
])

BATCH_ID_LITERAL = None
try:
    df_batch_id = spark.read.format("jdbc") \
        .option("url", oracle_url) \
        .option("query", "SELECT get_next_batch_id AS BATCH_ID_VAL FROM DUAL") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .option("fetchsize",1).load()

    batch_id_value = df_batch_id.collect()[0]["BATCH_ID_VAL"]
    BATCH_ID_LITERAL = int(batch_id_value)
    print(f"=== Fetched BATCH_ID: {BATCH_ID_LITERAL} ===")
except Exception as e:
    print(f"Error fetching BATCH_SEQ via JDBC: {e}")
    spark.stop()
    exit()

posting_date_str = datetime.date.today().strftime("%Y-%m-%d")
print(f"=== Using current date for POST_DATE: {posting_date_str} ===")


# base_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/"

# file_types_with_extra = ["INV", "PRI", "BOR", "PRB", "ONL", "AGRI_PY", "AGRI_CY", "INV_IN","INV_TD","INV_DC","BOR_DC"]
# file_types_without_extra = ["INV_BC", "INV_FD", "INV_RE", "GEN_FIT", "INV_B8", "INV_CT", "INV_ECG", "INV_FCI","BOR_VS","INV_GB"]
# paths_to_read = [f"{base_path}*GLIF{ft}_[0-9][0-9][0-9].gz" for ft in file_types_with_extra]
# paths_to_read.extend([f"{base_path}*GLIF{ft}.gz" for ft in file_types_without_extra])

# df_raw = spark.read.text(paths_to_read)


df_raw = spark.read.text("hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/VARAGA_GLIFONL_35*")

digit_map = create_map(
    lit('1'), lit('1'), lit('2'), lit('2'), lit('3'), lit('3'), lit('4'), lit('4'), lit('5'), lit('5'),
    lit('6'), lit('6'), lit('7'), lit('7'), lit('8'), lit('8'), lit('9'), lit('9'), lit('0'), lit('0'),
    lit('p'), lit('0'), lit('q'), lit('1'), lit('r'), lit('2'), lit('s'), lit('3'), lit('t'), lit('4'),
    lit('u'), lit('5'), lit('v'), lit('6'), lit('w'), lit('7'), lit('x'), lit('8'), lit('y'), lit('9')
)
df_processed = df_raw.withColumns({
    "Id": expr("substring(value, 51, 18)"),
    "currency_code": expr("substring(value, 48, 3)")
})
df_clean = df_processed.withColumns({
    "Amount_raw": when(col("currency_code") != "INR", expr("substring(value, 133, 17)"))
                 .otherwise(expr("substring(value, 116, 17)")),
    "last_char": when(col("currency_code") != "INR", expr("substring(value, 149, 1)"))
                 .otherwise(expr("substring(value, 132, 1)"))
}).drop("value", "currency_code")
df_clean = df_clean.withColumns({
    "Amount_base": substring(col("Amount_raw"), 1, 16),
    "sign_char": substring(col("Amount_raw"), 17, 1)
})
df_with_signed_amount = df_clean.withColumns({
    "last_digit": digit_map.getItem(col("sign_char")),
    "sign": when(col("sign_char").isin(['p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y']), -1).otherwise(1)
})
df_with_signed_amount = df_with_signed_amount.withColumn(
    "Amount_decimal_str",
    concat(col("Amount_base"), col("last_digit"))
)
df_final = df_with_signed_amount.withColumns({
    "Amount_final": (col("Amount_decimal_str").cast(DecimalType(22, 3)) / 1000) * col("sign")
})
df_with_signed_amount = df_final.withColumns({
    "Amountpve": when(df_final["Amount_final"] > 0, df_final["Amount_final"]).otherwise(0),
    "Amountnve": when(df_final["Amount_final"] < 0, df_final["Amount_final"]).otherwise(0)
})
df_agg = df_with_signed_amount.groupBy("Id").agg(
    F.sum(F.col("Amountpve")).alias("DEBIT_AMOUNT"),
    F.sum(F.col("Amountnve")).alias("CREDIT_AMOUNT"),
    F.count(F.col("Id")).alias("TRANSACTION_COUNT")
)


df_mapped = df_agg.withColumns({ 
    "BATCH_ID": lit(BATCH_ID_LITERAL).cast(StringType()), 
    # "TRANSACTION_DATE": F.to_date(F.lit(posting_date_str)),
    "JOURNAL_ID": lit(None).cast(StringType()),
    "POST_DATE": to_timestamp(lit(posting_date_str), "yyyy-MM-dd").cast("timestamp"),
    "BRANCH_CODE": substring(col("Id"), 1, 5),
    "CURRENCY": substring(col("Id"), 6, 3),
    "CGL": substring(col("Id"), 9, 10),
    "NARRATION": lit("CBS consolidated txns").cast(StringType()),
    "SOURCE_FLAG": lit("C").cast(StringType())
})

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


cgl_query = f"""
(
   SELECT CGL_NUMBER FROM cgl_master where BAL_COMPARE=1
) T1
"""
master_cgl_list = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", cgl_query) \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("batchsize", 110000) \
    .option("numPartitions", 50) \
    .load()

joined_df = df_final_schema.join(
    broadcast(master_cgl_list),
    df_final_schema["CGL"] == master_cgl_list["CGL_NUMBER"],
    "left_outer"
)


df_valid = joined_df.withColumn(
    "Validated_CGL",
    when(col("CGL_NUMBER").isNull(),
         when(substring(col("CGL"), 1, 1) == lit("5"), lit("5000000000")) 
         .otherwise(lit("1111111111"))
    ).otherwise(col("CGL_NUMBER")) # If join DID find a match, use the value from the master list
)
# master_cgl_list.show()
# df_valid.show(20, False)

df_valid1 = df_valid.select("BATCH_ID","JOURNAL_ID","POST_DATE","BRANCH_CODE","CURRENCY",(F.col("Validated_CGL")).alias("CGL"),"NARRATION","DEBIT_AMOUNT","CREDIT_AMOUNT","TRANSACTION_COUNT","SOURCE_FLAG")

df_summary = df_valid1.groupBy("CGL", "BRANCH_CODE", "CURRENCY").agg(
    F.sum(F.col("DEBIT_AMOUNT")).alias("Total_Aggregated_DEBIT"),
    F.sum(F.col("CREDIT_AMOUNT")).alias("Total_Aggregated_CREDIT"),
    F.count(F.col("TRANSACTION_COUNT")).alias("Total_Transaction_Count")
).alias("summary") 


join_keys = ["CGL", "BRANCH_CODE", "CURRENCY"]


df_final_with_all_details = df_valid1.join(
    df_summary,
    on=join_keys,
    how="left"
).withColumn("TRANSACTION_DATE",F.to_date(F.lit(today),"dd-MM-yy"))

print("Columns in the final DataFrame:")
print(df_final_with_all_details.columns)

df_final_with_all_details.show(100, False)







# print("==============result Dataframe======================")
# df_result.show()
# print("==============processed Dataframe======================")
# processed_df.show(truncate=False)

try:
    # df_result.write \
    #         .format("jdbc") \
    #         .option("url", oracle_url) \
    #         .option("dbtable", "GL_TRANSACTIONS") \
    #         .option("user", oracle_user) \
    #         .option("password", oracle_password) \
    #         .option("driver", oracle_driver) \
    #         .option("batchsize", 110000) \
    #         .option("numPartitions", 50) \
    #         .mode("append") \
    #         .save()

    print("=== Data successfully written to Oracle DB ===") 
except Exception as e:
    print(f"Error writing to Oracle DB: {e}")

    

# ====================================================================================================================================================================================================================================================================

# ====================================================================================================================================================================================================================================================================


# filter_date_str = '2025-11-06'
filter_date_str = yesterday

sql_query = f"""
(
    SELECT CGL,BALANCE,CURRENCY,BRANCH_CODE
    FROM GL_BALANCE 
    WHERE TRUNC(BALANCE_DATE) = TO_DATE('{filter_date_str}', 'YYYY-MM-DD')
) T1
"""




df_date_filtered = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", sql_query) \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("batchsize", 110000) \
    .option("numPartitions", 50) \
    .load()

# print("==============Fetched Dataframe======================")
# # df_date_filtered.show(100,False)

combined_df = processed_df.unionAll(df_date_filtered)

final_aggregated_df = combined_df.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(sum("BALANCE").alias("BALANCE")).orderBy("CGL", "BRANCH_CODE")
final_aggregated_df=final_aggregated_df.withColumn("BALANCE_DATE",F.to_date(F.lit(posting_date_str)))

print("==========================Final Dataframe=================")
final_aggregated_df.show(50,False)

final_aggregated_df.write \
    .format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "GL_BALANCE") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .option("batchsize", 110000) \
    .option("numPartitions", 50) \
    .save()

spark.stop()

