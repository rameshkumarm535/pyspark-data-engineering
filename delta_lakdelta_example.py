from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaExample") \
    .getOrCreate()

data = [(1,"A",100),(2,"B",200)]
df = spark.createDataFrame(data, ["id","name","amount"])

# Write as Delta
df.write.format("delta").mode("overwrite").save("delta_table")

# Read Delta
delta_df = spark.read.format("delta").load("delta_table")

delta_df.show()

# Merge example (Upsert)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "delta_table")

updates = spark.createDataFrame([(2,"B",250)],["id","name","amount"])

delta_table.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
