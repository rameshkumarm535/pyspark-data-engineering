from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DAGExample").getOrCreate()

data = [(1,100),(2,200),(3,300)]
df = spark.createDataFrame(data, ["id","amount"])

df1 = df.filter(col("amount") > 100)
df2 = df1.withColumn("tax", col("amount") * 0.1)
df3 = df2.groupBy().sum("tax")

df3.explain(True)   # Shows DAG & Physical Plan
df3.show()
