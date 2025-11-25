from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 60)
print("WELCOME TO PYSPARK LEARNING!")
print("=" * 60)

# Create sample data
data = [
    ("Alice", 25, 60000),
    ("Bob", 30, 75000),
    ("Charlie", 35, 90000),
    ("Diana", 28, 70000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# Display
print("\n✓ All Employees:")
df.show()

# Filter
print("\n✓ Employees older than 28:")
df.filter(df.Age > 28).show()

# Aggregate
print("\n✓ Average Salary:")
from pyspark.sql.functions import avg
df.agg(avg("Salary")).show()

# Add column
print("\n✓ With 10% Bonus:")
from pyspark.sql.functions import col
df.withColumn("Bonus", col("Salary") * 0.1).select("Name", "Salary", "Bonus").show()

print("\n✓ Learning PySpark! 🎉")

spark.stop()
