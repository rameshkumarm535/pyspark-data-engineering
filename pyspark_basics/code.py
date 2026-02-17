from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PySparkBasics").getOrCreate()

data = [(1,"A",100),
        (2,"B",200),
        (2,"B",200),
        (3,"C",None)]

df = spark.createDataFrame(data, ["id","name","amount"])

# Remove nulls
df_clean = df.dropna()

# Remove duplicates
df_dedup = df_clean.dropDuplicates(["id"])

# Filter records
df_filtered = df_dedup.filter(col("amount") > 100)

df_filtered.show()
