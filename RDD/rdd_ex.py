from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("RDDEx")
sc = SparkContext(conf=conf)
spark: SparkSession = SparkSession.builder.appName("RDDEx").getOrCreate()

# * (name, age) 형태의 RDD 생성
dataRDD = sc.parallelize([("Brooke", 20), ("Danny", 31), ("Jules", 30), ("Brooke", 25)])

# * 집계와 평균을 위한 람다 표현식과 함께 map과 reduceByKey 트랜스포메이션 사용
agesRDD = (
    dataRDD.map(lambda x: (x[0], (x[1], 1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0], x[1][0] / x[1][1]))
)
df = agesRDD.toDF()
df.show()
"""
+------+----+
|    _1|  _2|
+------+----+
|Brooke|22.5|
| Danny|31.0|
| Jules|30.0|
+------+----+
"""

data_df = spark.createDataFrame(
    data=[("Brooke", 20), ("Danny", 31), ("Jules", 30), ("Brooke", 25)], schema=["name", "age"]
)
avg_df = data_df.groupBy("name").avg("age")
avg_df.show()
"""
+------+--------+
|  name|avg(age)|
+------+--------+
|Brooke|    22.5|
| Danny|    31.0|
| Jules|    30.0|
+------+--------+
"""
