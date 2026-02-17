# Databricks Notebook Example

from pyspark.sql.functions import current_timestamp

df = spark.read.format("delta").load("/mnt/delta/sales")

df_transformed = df.withColumn("processed_time", current_timestamp())

df_transformed.write.format("delta").mode("overwrite").save("/mnt/delta/sales_gold")
