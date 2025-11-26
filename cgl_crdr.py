from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, broadcast, to_timestamp
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
from decimal import Decimal

# =====================================================================
# SPARK SESSION
# =====================================================================

spark = SparkSession.builder \
    .appName("CBS_GL_20Row_Test") \
    .config("spark.jars", "./ojdbc8.jar") \
    .getOrCreate()

# =====================================================================
# CONFIGURATIONS
# =====================================================================

WRITE_PARTITIONS = 5
BATCHSIZE_JDBC = 10000

oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1"
oracle_user = "fincore"
oracle_password = "Password#1234"
oracle_driver = "oracle.jdbc.driver.OracleDriver"

today = date.today()
yesterday = today - timedelta(days=1)
posting_date_str = today.strftime("%Y-%m-%d")
filter_date_str = yesterday.strftime("%Y-%m-%d")

# =====================================================================
# SAMPLE 20 ROW DATA (STARTS HERE)
# =====================================================================

schema = StructType([
    StructField("BATCH_ID", StringType(), True),
    StructField("JOURNAL_ID", StringType(), True),
    StructField("POST_DATE", TimestampType(), True),
    StructField("BRANCH_CODE", StringType(), True),
    StructField("CURRENCY", StringType(), True),
    StructField("CGL", StringType(), True),
    StructField("NARRATION", StringType(), True),
    StructField("DEBIT_AMOUNT", DecimalType(20,2), True),
    StructField("CREDIT_AMOUNT", DecimalType(20,2), True),
    StructField("TRANSACTION_COUNT", LongType(), True),
    StructField("SOURCE_FLAG", StringType(), True)
])

data = [
    ("250", None, datetime(2025,11,25), "15779", "INR", "5106505001", "CBS consolidated txns", Decimal("4633292.34"), Decimal("0.00"), 2293, "C"),
    ("250", None, datetime(2025,11,25), "06851", "INR", "1106505001", "CBS consolidated txns", Decimal("4708426.21"), Decimal("0.00"), 2810, "C"),
    ("250", None, datetime(2025,11,25), "04545", "INR", "1106505001", "CBS consolidated txns", Decimal("405.00"), Decimal("-10731.06"), 11, "C"),
    ("250", None, datetime(2025,11,25), "32291", "INR", "2028070603", "CBS consolidated txns", Decimal("55651.00"), Decimal("-426620.92"), 83, "C"),
    ("250", None, datetime(2025,11,25), "16000", "INR", "1106505001", "CBS consolidated txns", Decimal("100.00"), Decimal("-34136.00"), 6, "C"),
    ("250", None, datetime(2025,11,25), "06659", "INR", "2014070602", "CBS consolidated txns", Decimal("90694.20"), Decimal("-70364.20"), 192, "C"),
    ("250", None, datetime(2025,11,25), "11223", "INR", "5001234567", "CBS consolidated txns", Decimal("12345.67"), Decimal("-23456.78"), 45, "C"),
    ("250", None, datetime(2025,11,25), "99887", "INR", "6200123456", "CBS consolidated txns", Decimal("74620.50"), Decimal("-12800.30"), 210, "C"),
    ("250", None, datetime(2025,11,25), "44556", "INR", "1204500001", "CBS consolidated txns", Decimal("998.00"), Decimal("-1200.45"), 17, "C"),
    ("250", None, datetime(2025,11,25), "88771", "INR", "6363123456", "CBS consolidated txns", Decimal("25000.75"), Decimal("-46000.00"), 92, "C"),

    # extra 10 rows
    ("250", None, datetime(2025,11,25), "22111", "INR", "5106505001", "CBS consolidated txns", Decimal("100000.00"), Decimal("0.00"), 52, "C"),
    ("250", None, datetime(2025,11,25), "22111", "INR", "6200123456", "CBS consolidated txns", Decimal("0.00"), Decimal("-15000.50"), 19, "C"),
    ("250", None, datetime(2025,11,25), "55331", "INR", "5001234567", "CBS consolidated txns", Decimal("821.11"), Decimal("-127.90"), 8, "C"),
    ("250", None, datetime(2025,11,25), "55331", "INR", "1106505001", "CBS consolidated txns", Decimal("5500.00"), Decimal("0.00"), 33, "C"),
    ("250", None, datetime(2025,11,25), "77551", "INR", "1204500001", "CBS consolidated txns", Decimal("0.00"), Decimal("-7000.00"), 12, "C"),
    ("250", None, datetime(2025,11,25), "77551", "INR", "2028070603", "CBS consolidated txns", Decimal("35000.00"), Decimal("0.00"), 28, "C"),
    ("250", None, datetime(2025,11,25), "88990", "INR", "2014070602", "CBS consolidated txns", Decimal("0.00"), Decimal("-900.00"), 5, "C"),
    ("250", None, datetime(2025,11,25), "88990", "INR", "6363123456", "CBS consolidated txns", Decimal("12500.00"), Decimal("0.00"), 33, "C"),
    ("250", None, datetime(2025,11,25), "77411", "INR", "5001234567", "CBS consolidated txns", Decimal("2300.00"), Decimal("-100.00"), 9, "C"),
    ("250", None, datetime(2025,11,25), "77411", "INR", "6200123456", "CBS consolidated txns", Decimal("0.00"), Decimal("-2600.00"), 13, "C"),
]

df_final_schema = spark.createDataFrame(data, schema)
df_final_schema.show(truncate=False)

# =====================================================================
# CGL MASTER (from Oracle)
# =====================================================================

# cgl_master = spark.read.format("jdbc") \
#     .option("url", oracle_url) \
#     .option("dbtable", "(SELECT CGL_NUMBER FROM cgl_master WHERE BAL_COMPARE=1)") \
#     .option("user", oracle_user) \
#     .option("password", oracle_password) \
#     .option("driver", oracle_driver) \
#     .load() \
#     .select(col("CGL_NUMBER").alias("CGL")).distinct()

from pyspark.sql.types import StructType, StructField, StringType

schema_cgl = StructType([
    StructField("CGL_NUMBER", StringType(), True)
])

cgl_master_data = [
    ("2028070603",),
    ("2014070602",),
    ("5001234567",),
    ("6200123456",),
    ("1204500001",),
    ("6363123456",),
    ("1106505001",),
    ("5106505001",),
    ("1106505002",),
    ("6200123499",),
]

cgl_master = spark.createDataFrame(cgl_master_data, schema_cgl)

# =====================================================================
# VALIDATE CGL
# =====================================================================

df_validated = df_final_schema.join(
    broadcast(cgl_master.select(col("CGL_NUMBER").alias("CGL")).withColumn("IS_VALID", lit(1))),
    on="CGL",
    how="left"
).withColumn(
    "VALIDATED_CGL",
    when(col("IS_VALID").isNotNull(), col("CGL"))
    .when(col("CGL").startswith("5"), lit("5000000000"))
    .otherwise(lit("1111111111"))
)

# =====================================================================
# AGGREGATION BY VALIDATED_CGL
# =====================================================================

result = df_validated.groupBy("VALIDATED_CGL").agg(
    F.sum("DEBIT_AMOUNT").alias("TOTAL_DEBIT"),
    F.sum("CREDIT_AMOUNT").alias("TOTAL_CREDIT"),
    F.sum("TRANSACTION_COUNT").alias("TOTAL_TRANSACTIONS")
)

# =====================================================================
# SYNTHETIC BALANCING ROWS
# =====================================================================

result_net = result.withColumn("NET", col("TOTAL_DEBIT") + col("TOTAL_CREDIT"))

synthetic = result_net.filter(col("NET") != 0).select(
    col("VALIDATED_CGL"),
    when(col("NET") < 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_DEBIT"),
    when(col("NET") > 0, -col("NET")).otherwise(lit(0)).alias("TOTAL_CREDIT"),
    lit(1).alias("TOTAL_TRANSACTIONS")
)

final_balanced = result.unionByName(synthetic)

final_balanced_agg = final_balanced.groupBy("VALIDATED_CGL").agg(
    F.sum("TOTAL_DEBIT").alias("TOTAL_DEBIT"),
    F.sum("TOTAL_CREDIT").alias("TOTAL_CREDIT"),
    F.sum("TOTAL_TRANSACTIONS").alias("TOTAL_TRANSACTIONS")
).orderBy("VALIDATED_CGL")

final_balanced.show(truncate=False)
final_balanced_agg.show(truncate=False)

# =====================================================================
# STOP HERE FOR TESTING
# =====================================================================

print("========== SAMPLE RUN COMPLETED SUCCESSFULLY ==========")



# +-------------+-----------+------------+------------------+
# |VALIDATED_CGL|TOTAL_DEBIT|TOTAL_CREDIT|TOTAL_TRANSACTIONS|
# +-------------+-----------+------------+------------------+
# |1106505001   |4714431.21 |-44867.06   |2860              |
# |5106505001   |4733292.34 |0.00        |2345              |
# |2028070603   |90651.00   |-426620.92  |111               |
# |2014070602   |90694.20   |-71264.20   |197               |
# |6200123456   |74620.50   |-30400.80   |242               |
# |5001234567   |15466.78   |-23684.68   |62                |
# |1204500001   |998.00     |-8200.45    |29                |
# |6363123456   |37500.75   |-46000.00   |125               |
# |1106505001   |0.00       |-4669564.15 |0                 |
# |5106505001   |0.00       |-4733292.34 |0                 |
# |2028070603   |335969.92  |0.00        |0                 |
# |2014070602   |0.00       |-19430.00   |0                 |
# |6200123456   |0.00       |-44219.70   |0                 |
# |5001234567   |8217.90    |0.00        |0                 |
# |1204500001   |7202.45    |0.00        |0                 |
# |6363123456   |8499.25    |0.00        |0                 |
# +-------------+-----------+------------+------------------+