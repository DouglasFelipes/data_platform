from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DGZIN_JOB").getOrCreate()

df = spark.range(0, 1000000)
result = df.groupBy().sum()

result.show()

spark.stop()