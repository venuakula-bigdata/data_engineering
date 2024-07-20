from pyspark.sql import SparkSession

from pyspark.sql.functions import col, rank

from pyspark.sql.window import Window

spark = SparkSession.builder.appName("customerdf").getOrCreate()

df = spark.read.option("header", "true").csv("customers.csv")

df1 = df.groupby("country").count()

window_spec = Window.orderBy(col("count").desc())

df2 = df1.withColumn("rank", rank().over(window_spec))

df3 = df2.where(df2.rank == 3)

df3.show()
