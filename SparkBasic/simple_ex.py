from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
)

strings = spark.read.text("../README.md")
strings.show(10, False)
print(strings.count())
