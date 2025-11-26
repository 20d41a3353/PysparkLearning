from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, LongType
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.master("local[*]").appName("newTask").getOrCreate()

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
    ("252", None, datetime(2025,11,25), "15779", "INR", "5106505001", "CBS consolidated txns", Decimal("4633292.34"), Decimal("-2304292.55"), 2293, "C"),
    ("252", None, datetime(2025,11,25), "06851", "INR", "1106505001", "CBS consolidated txns", Decimal("4708426.21"), Decimal("-2789806.95"), 2810, "C"),
    ("252", None, datetime(2025,11,25), "04545", "INR", "1106505001", "CBS consolidated txns", Decimal("405.00"), Decimal("-10731.06"), 11, "C"),
    ("252", None, datetime(2025,11,25), "32291", "INR", "2028070603", "CBS consolidated txns", Decimal("55651.00"), Decimal("-426620.92"), 83, "C"),
    ("252", None, datetime(2025,11,25), "16000", "INR", "1106505001", "CBS consolidated txns", Decimal("100.00"), Decimal("-34136.00"), 6, "C"),
    ("252", None, datetime(2025,11,25), "06659", "INR", "2014070602", "CBS consolidated txns", Decimal("90694.20"), Decimal("-70364.20"), 192, "C"),
    ("252", None, datetime(2025,11,25), "11223", "INR", "5001234567", "CBS consolidated txns", Decimal("12345.67"), Decimal("-23456.78"), 45, "C"),
    ("252", None, datetime(2025,11,25), "99887", "INR", "6200123456", "CBS consolidated txns", Decimal("74620.50"), Decimal("-12800.30"), 210, "C"),
    ("252", None, datetime(2025,11,25), "44556", "INR", "1204500001", "CBS consolidated txns", Decimal("998.00"), Decimal("-1200.45"), 17, "C"),
    ("252", None, datetime(2025,11,25), "88771", "INR", "6363123456", "CBS consolidated txns", Decimal("25000.75"), Decimal("-46000.00"), 92, "C"),
]


df_10 = spark.createDataFrame(data, schema)
df_10.show(truncate=False)

# Create a single-column DataFrame for CGL codes: use one-element tuples
schemacode = StructType([
    StructField("CGL", StringType(), True)
])

# Each element must be a tuple for a single-field schema (note trailing comma)
codes = [
    ("2028070603",),
    ("2014070602",),
    ("5001234567",),
    ("6200123456",),
    ("1204500001",),
]

df_codes = spark.createDataFrame(codes, schemacode)
df_codes.show(truncate=False)

print("\n" + "="*80)
print("Step 1: Left join df_10 with df_codes on CGL")
print("="*80)

# Left join df_10 with df_codes to mark valid CGLs
# Use broadcast for df_codes since it's much smaller (10k rows vs 100M rows)
from pyspark.sql.functions import broadcast, when, col

df_merged = df_10.join(
    broadcast(df_codes.select("CGL").distinct().withColumn("IS_VALID", col("CGL"))),
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

# print("\nCheck: TOTAL_DEBIT + TOTAL_CREDIT should be 0 for all groups")
# # final.withColumn("CHECK", col("TOTAL_DEBIT") + col("TOTAL_CREDIT")).show(truncate=False)

# # Produce unioned DataFrame but use column name `CGL` for clarity and downstream compatibility
# union_by_cgl = final.withColumnRenamed("VALIDATED_CGL", "CGL").select(
#     "CGL", "TOTAL_DEBIT", "TOTAL_CREDIT", "TOTAL_TRANSACTIONS"
# ).orderBy("CGL")

# print("\nUnioned DataFrame (by CGL):")
# union_by_cgl.show(truncate=False)

